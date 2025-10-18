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
from urllib.parse import urlparse, urlunparse
import xml.etree.ElementTree as ET


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
    subparsers = parser.add_subparsers(dest="command")

    # generate mode
    gen = subparsers.add_parser(
        "generate",
        help="Generate a CSV from a site's sitemap with staging URLs",
    )
    gen.add_argument("site", help="Site root or sitemap.xml URL (e.g., https://www.example.com)")
    gen.add_argument(
        "--staging-base",
        required=True,
        help="Staging base host (e.g., staging-fringe.webflow.io). 'https://' is auto-added.",
    )
    gen.add_argument(
        "-o",
        "--output",
        help="Output CSV path. Defaults to 'sitemap-checked-<timestamp>.csv' in CWD.",
    )
    gen.add_argument(
        "--no-precheck",
        action="store_true",
        help="Skip HTTP checks during generation (by default we pre-check live & staging)",
    )

    # check/update mode (default)
    parser.add_argument(
        "-i",
        "--input",
        required=False,
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
    url_matches_value: str = "",
) -> str:
    scope_normalized = (scope_value or "").strip().lower()
    status_normalized = (status_value or "").strip().lower()
    url_matches_normalized = (url_matches_value or "").strip().lower()

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
        # If URL Matches is 'Yes', we prefer 'No' rather than '404' to avoid noise
        if url_matches_normalized in ("yes", "y", "true"):  # treat as a structural match
            return "No"
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


def ensure_https(url: str) -> str:
    if not url:
        return url
    parsed = urlparse(url)
    if not parsed.scheme:
        return f"https://{url}"
    return url


def build_staging_url(live_url: str, staging_base_host: str) -> str:
    staging_base_host = staging_base_host.strip().rstrip("/")
    live_url = ensure_https(live_url)
    parsed = urlparse(live_url)
    path = parsed.path or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"https://{staging_base_host}{path}{query}"


def parse_sitemap_content(xml_bytes: bytes) -> List[str]:
    urls: List[str] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return urls
    tag = root.tag.lower()
    if tag.endswith("sitemapindex"):
        for sm in root.findall("{*}sitemap"):
            loc_el = sm.find("{*}loc")
            if loc_el is not None and loc_el.text:
                try:
                    with urlopen(ensure_https(loc_el.text), timeout=15) as resp:
                        urls.extend(parse_sitemap_content(resp.read()))
                except Exception:
                    continue
    elif tag.endswith("urlset"):
        for u in root.findall("{*}url"):
            loc_el = u.find("{*}loc")
            if loc_el is not None and loc_el.text:
                urls.append(loc_el.text.strip())
    return urls


def fetch_sitemap_urls(site_or_sitemap: str) -> List[str]:
    base = ensure_https(site_or_sitemap.strip())
    try_urls = [base]
    if not base.lower().endswith(".xml"):
        # Try common sitemap location
        base_no_slash = base.rstrip("/")
        try_urls = [f"{base_no_slash}/sitemap.xml"]
    urls: List[str] = []
    for candidate in try_urls:
        try:
            with urlopen(candidate, timeout=15) as resp:
                content = resp.read()
            urls = parse_sitemap_content(content)
            if urls:
                break
        except Exception:
            continue
    return sorted(set(urls))


def is_ok(status_code: Optional[int]) -> bool:
    return status_code is not None and 200 <= status_code < 400


def generate_rows_from_sitemap(
    site_or_sitemap: str,
    staging_base_host: str,
    precheck: bool = True,
) -> Tuple[List[Dict[str, str]], List[str]]:
    live_urls = fetch_sitemap_urls(site_or_sitemap)
    fieldnames = [
        "Live Site URL",
        "Theoretical Staging Link",
        "Page Exists?",
        "URL Matches",
        "Redirect URL",
        "Scope",
        "Status",
    ]
    rows: List[Dict[str, str]] = []
    if not live_urls:
        return rows, fieldnames

    # Precompute staging URLs
    staging_urls = [build_staging_url(live, staging_base_host) for live in live_urls]

    live_status: List[Optional[int]] = [None] * len(live_urls)
    staging_status: List[Optional[int]] = [None] * len(staging_urls)

    if precheck:
        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = {}
            for i, u in enumerate(live_urls):
                futures[executor.submit(fetch_status, ensure_https(u), 10.0, DEFAULT_USER_AGENT)] = ("live", i)
            for i, u in enumerate(staging_urls):
                futures[executor.submit(fetch_status, ensure_https(u), 10.0, DEFAULT_USER_AGENT)] = ("staging", i)
            for fut in as_completed(futures):
                kind, idx = futures[fut]
                try:
                    res = fut.result()
                    code = res.status_code
                except Exception:
                    code = None
                if kind == "live":
                    live_status[idx] = code
                else:
                    staging_status[idx] = code

    for i, live in enumerate(live_urls):
        staging = staging_urls[i]
        page_exists = "Yes" if is_ok(staging_status[i]) else "No"
        url_matches = "Yes" if (is_ok(live_status[i]) and is_ok(staging_status[i])) else "Page Does Not Exist"
        rows.append(
            {
                "Live Site URL": live,
                "Theoretical Staging Link": staging,
                "Page Exists?": page_exists,
                "URL Matches": url_matches,
                "Redirect URL": "",
                "Scope": "",
                "Status": "",
            }
        )
    return rows, fieldnames


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

    # Generation mode
    if args.command == "generate":
        rows, fieldnames = generate_rows_from_sitemap(
            site_or_sitemap=args.site,
            staging_base_host=args.staging_base,
            precheck=(not args.no_precheck),
        )
        if not rows:
            print("No URLs found in sitemap", file=sys.stderr)
            return 2
        # Output path
        out = args.output
        if not out:
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            out = os.path.abspath(f"sitemap-checked-{timestamp}.csv")
        try:
            write_csv_rows(out, fieldnames, rows)
        except Exception as e:
            print(f"Failed to write generated CSV: {e}", file=sys.stderr)
            return 3
        print(f"Generated CSV: {out} (rows={len(rows)})")
        return 0

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

    required_columns_min = {"Theoretical Staging Link", "Page Exists?"}
    missing = [c for c in required_columns_min if c not in (fieldnames or [])]
    if missing:
        print("Missing required column(s) in CSV: " + ", ".join(missing), file=sys.stderr)
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
        url_matches_value = row.get("URL Matches", "")
        crawl_result = index_to_result.get(idx)
        status_code = crawl_result.status_code if crawl_result else None

        page_exists_value = decide_page_exists_value(
            status_code, scope_value, status_value, url_matches_value
        )
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


