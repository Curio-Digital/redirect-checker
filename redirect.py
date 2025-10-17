#!/usr/bin/env python3

import argparse
import csv
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)


@dataclass
class CrawlResult:
    url: str
    status_code: Optional[int]
    error: Optional[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check Theoretical Staging Links from a CSV and update 'Page Exists?'"
    )
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Path to input CSV (e.g. 'FRI - Full Sitemap - Redirect Planning.csv')",
    )
    parser.add_argument(
        "-o",
        "--output",
        help=(
            "Path to output CSV. Defaults to '<input basename>-checked.csv' next to the input file."
        ),
    )
    parser.add_argument(
        "-j",
        "--concurrency",
        type=int,
        default=20,
        help="Number of concurrent HTTP checks (default: 20)",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=float,
        default=10.0,
        help="Per-request timeout in seconds (default: 10)",
    )
    parser.add_argument(
        "-A",
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent header to send (default: a Safari-like UA)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Print progress and per-URL results",
    )
    parser.add_argument(
        "--pasteable",
        action="store_true",
        help="Also produce a newline-separated 'Page Exists?' column file for pasting",
    )
    parser.add_argument(
        "--pasteable-file",
        help="Optional path to write pasteable column values (defaults beside output CSV)",
    )
    return parser.parse_args()


def decide_page_exists_value(
    status_code: Optional[int],
    scope_value: str,
    status_value: str,
) -> str:
    scope_normalized = (scope_value or "").strip().lower()
    status_normalized = (status_value or "").strip().lower()

    is_in_scope = any(
        token in scope_normalized for token in ("in scope",)
    ) or any(
        token in status_normalized for token in ("in scope", "added to initial scope", "needed for launch")
    )

    not_needed = any(
        token in scope_normalized for token in ("not in scope",)
    ) or ("not needed" in status_normalized)

    if status_code is None:
        return "No" if not_needed else ("404" if is_in_scope else "No")

    if 200 <= status_code < 400:
        return "Yes"

    if status_code == 404:
        return "No" if not_needed else ("404" if is_in_scope else "No")

    return "No"


def fetch_status(
    url: str,
    timeout: float,
    user_agent: str,
) -> CrawlResult:
    if not url:
        return CrawlResult(url=url, status_code=None, error="empty-url")

    try:
        req = Request(url, headers={"User-Agent": user_agent})
        with urlopen(req, timeout=timeout) as resp:
            return CrawlResult(url=url, status_code=getattr(resp, "status", 200), error=None)
    except HTTPError as e:
        return CrawlResult(url=url, status_code=e.code, error=str(e))
    except URLError as e:
        return CrawlResult(url=url, status_code=None, error=str(e))
    except Exception as e:  # pragma: no cover
        return CrawlResult(url=url, status_code=None, error=str(e))


def compute_output_path(input_path: str, override_output: Optional[str]) -> str:
    if override_output:
        return override_output
    parent, base = os.path.split(input_path)
    name, ext = os.path.splitext(base)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    return os.path.join(parent, f"{name}-checked-{timestamp}{ext or '.csv'}")


def read_csv_rows(input_path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    with open(input_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows: List[Dict[str, str]] = [dict(row) for row in reader]
        fieldnames = reader.fieldnames or []
    return rows, fieldnames


def write_csv_rows(output_path: str, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    args = parse_args()

    input_path = args.input
    output_path = compute_output_path(input_path, args.output)
    timeout = float(args.timeout)
    concurrency = max(1, int(args.concurrency))
    user_agent = args.user_agent
    verbose = bool(getattr(args, "verbose", False))

    if verbose:
        print(f"Reading CSV: {input_path}")
    try:
        rows, fieldnames = read_csv_rows(input_path)
    except FileNotFoundError:
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 2

    required_columns = {
        "Theoretical Staging Link",
        "Page Exists?",
        "Scope",
        "Status",
    }
    missing = [c for c in required_columns if c not in (fieldnames or [])]
    if missing:
        print(
            "Missing required column(s) in CSV: " + ", ".join(missing),
            file=sys.stderr,
        )
        return 2

    urls_to_check: List[Tuple[int, str]] = []
    for idx, row in enumerate(rows):
        url = (row.get("Theoretical Staging Link") or "").strip()
        if url:
            urls_to_check.append((idx, url))

    index_to_result: Dict[int, CrawlResult] = {}
    if urls_to_check:
        if verbose:
            print(
                f"Checking {len(urls_to_check)} URLs with concurrency={concurrency}, timeout={timeout}s"
            )
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_info = {
                executor.submit(fetch_status, url, timeout, user_agent): (idx, url)
                for idx, url in urls_to_check
            }
            completed = 0
            total = len(future_to_info)
            for future in as_completed(future_to_info):
                idx, url = future_to_info[future]
                try:
                    result = future.result()
                except Exception as e:  # pragma: no cover
                    result = CrawlResult(url="", status_code=None, error=str(e))
                index_to_result[idx] = result
                completed += 1
                if verbose:
                    status_repr = (
                        str(result.status_code)
                        if result.status_code is not None
                        else f"error: {result.error}"
                    )
                    print(f"[{completed}/{total}] {url} -> {status_repr}")

    yes_count = 0
    no_count = 0
    not_found_count = 0
    pasteable_values: List[str] = []
    for idx, row in enumerate(rows):
        scope_value = row.get("Scope", "")
        status_value = row.get("Status", "")
        crawl_result = index_to_result.get(idx)
        status_code = crawl_result.status_code if crawl_result else None

        page_exists_value = decide_page_exists_value(status_code, scope_value, status_value)
        row["Page Exists?"] = page_exists_value
        pasteable_values.append(page_exists_value)
        if verbose:
            theoretical_url = (row.get("Theoretical Staging Link") or "").strip()
            print(
                f"Classified: {theoretical_url or '<empty>'} -> Page Exists?={page_exists_value}"
            )
        if page_exists_value == "Yes":
            yes_count += 1
        elif page_exists_value == "404":
            not_found_count += 1
        else:
            no_count += 1

    try:
        write_csv_rows(output_path, fieldnames, rows)
    except Exception as e:  # pragma: no cover
        print(f"Failed to write output CSV: {e}", file=sys.stderr)
        return 3

    if verbose:
        print(
            f"Summary: Yes={yes_count}, 404={not_found_count}, No={no_count} across {len(rows)} rows"
        )
    print(f"Wrote: {output_path}")

    if bool(getattr(args, "pasteable", False)):
        pasteable_path = args.pasteable_file
        if not pasteable_path:
            parent, base = os.path.split(output_path)
            name, _ext = os.path.splitext(base)
            pasteable_path = os.path.join(parent, f"{name}-page-exists.txt")
        try:
            with open(pasteable_path, "w", encoding="utf-8") as f:
                for value in pasteable_values:
                    f.write(f"{value}\n")
            print(f"Pasteable column written to: {pasteable_path}")
        except Exception as e:  # pragma: no cover
            print(f"Failed to write pasteable file: {e}", file=sys.stderr)
            return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


