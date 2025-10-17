import csv
import io
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

import streamlit as st
try:
    # When run as part of a package (python -m ...)
    from . import redirect as redirect_mod  # type: ignore
except Exception:
    # When run directly via `streamlit run python/redirect_ui.py`
    import sys
    sys.path.append(os.path.dirname(__file__))
    import redirect as redirect_mod  # type: ignore


def read_csv_from_bytes(data: bytes) -> Tuple[List[Dict[str, str]], List[str]]:
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows: List[Dict[str, str]] = [dict(row) for row in reader]
    fieldnames = reader.fieldnames or []
    return rows, fieldnames


def write_csv_to_bytes(fieldnames: List[str], rows: List[Dict[str, str]]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def run_checks(rows: List[Dict[str, str]], concurrency: int, timeout: float, user_agent: str) -> Tuple[List[Dict[str, str]], List[str]]:
    urls_to_check: List[Tuple[int, str]] = []
    for idx, row in enumerate(rows):
        url = (row.get("Theoretical Staging Link") or "").strip()
        if url:
            urls_to_check.append((idx, url))

    index_to_result: Dict[int, redirect_mod.CrawlResult] = {}
    progress = st.progress(0.0, text="Starting...")
    total = len(urls_to_check) or 1
    completed = 0

    if urls_to_check:
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
            future_to_info = {
                executor.submit(redirect_mod.fetch_status, url, timeout, user_agent): (idx, url)
                for idx, url in urls_to_check
            }
            for future in as_completed(future_to_info):
                idx, url = future_to_info[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = redirect_mod.CrawlResult(url=url, status_code=None, error=str(e))
                index_to_result[idx] = result
                completed += 1
                progress.progress(min(1.0, completed / total), text=f"Checked {completed}/{total}")

    pasteable_values: List[str] = []
    for idx, row in enumerate(rows):
        scope_value = row.get("Scope", "")
        status_value = row.get("Status", "")
        crawl_result = index_to_result.get(idx)
        status_code = crawl_result.status_code if crawl_result else None
        value = redirect_mod.decide_page_exists_value(status_code, scope_value, status_value)
        row["Page Exists?"] = value
        pasteable_values.append(value)

    return rows, pasteable_values


def main() -> None:
    st.set_page_config(page_title="Redirect Checker", page_icon="✅", layout="wide")
    st.title("Redirect Checker")
    st.caption("Upload a CSV, check staging links, and download the updated file.")

    with st.sidebar:
        st.header("Settings")
        concurrency = st.slider("Concurrency", min_value=1, max_value=64, value=20)
        timeout = st.number_input("Timeout (seconds)", min_value=1.0, max_value=60.0, value=10.0, step=1.0)
        user_agent = st.text_input("User-Agent", value=redirect_mod.DEFAULT_USER_AGENT)

    uploaded = st.file_uploader("Upload CSV", type=["csv"])

    if not uploaded:
        st.info("Select a CSV with columns: 'Theoretical Staging Link', 'Page Exists?', 'Scope', 'Status'.")
        return

    rows, fieldnames = read_csv_from_bytes(uploaded.read())
    if not rows:
        st.warning("No rows detected in the uploaded CSV.")
        return

    missing = [c for c in ("Theoretical Staging Link", "Page Exists?", "Scope", "Status") if c not in (fieldnames or [])]
    if missing:
        st.error("Missing required column(s): " + ", ".join(missing))
        return

    st.write(f"Rows detected: {len(rows)}")
    if st.button("Run Checks", type="primary"):
        start = time.time()
        updated_rows, pasteable_values = run_checks(rows, concurrency, float(timeout), user_agent)
        elapsed = time.time() - start

        st.success(f"Done in {elapsed:.1f}s")

        csv_bytes = write_csv_to_bytes(fieldnames, updated_rows)
        ts = time.strftime("%Y%m%d-%H%M%S")
        base = os.path.splitext(uploaded.name)[0]
        out_name = f"{base}-checked-{ts}.csv"
        st.download_button("Download Updated CSV", data=csv_bytes, file_name=out_name, mime="text/csv")

        st.subheader("Pasteable 'Page Exists?' column")
        st.caption("Copy all and paste into your Google Sheet column")
        paste_text = "\n".join(pasteable_values)
        st.text_area(label="Values", value=paste_text, height=200)
        st.download_button(
            "Download Pasteable Text",
            data=paste_text.encode("utf-8"),
            file_name=f"{base}-page-exists-{ts}.txt",
            mime="text/plain",
        )


if __name__ == "__main__":
    main()


