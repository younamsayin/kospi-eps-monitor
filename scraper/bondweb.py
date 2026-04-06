"""
Scrapes bondweb.co.kr research center for analyst reports.
Uses the internal AJAX endpoint that the page calls on load.
"""

import re
import time
import httpx
from bs4 import BeautifulSoup
from datetime import date
from typing import Optional
from urllib.parse import quote

BASE_URL = "https://www.bondweb.co.kr/MOA/Board/ResearchCenterV2"
LIST_URL = f"{BASE_URL}/AjaxPrimeListSub.asp"
DOWNLOAD_URL = f"{BASE_URL}/DownloadPage.asp"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": f"{BASE_URL}/PrimeSub01.asp?SubDiv=Sub110",
    "Content-Type": "application/x-www-form-urlencoded",
}

LIST_PARAMS = {
    "selMnuT": "0^1^0^0^0^0^0^0^0^0^0",
    "selMnuB": "1^1^1^1^1",
    "actNum": "0",
    "NWMnu": "01",
    "SubDiv": "Sub110",
    "lstNumN": "0",
    "lstNumO": "0",
    "ListEOF": "0",
}


def fetch_recent_reports(pages: int = 3, ticker_whitelist: Optional[set] = None) -> list:
    """
    Fetches recent analyst reports from bondweb research center.

    Args:
        pages: Number of pages to fetch (each page ~20 reports)
        ticker_whitelist: If provided, filter by matching company names against KOSPI 200

    Returns:
        List of report dicts: {ticker, company, broker, title, report_url, report_date, report_id}
    """
    if ticker_whitelist is not None and isinstance(ticker_whitelist, dict):
        return _search_reports_by_companies(ticker_whitelist, pages=pages)

    reports = []
    today = date.today()

    with httpx.Client(timeout=30, headers=HEADERS) as client:
        for _ in range(max(1, pages)):
            page_html = _fetch_list_html(client, LIST_PARAMS)
            page_reports = _parse_list(page_html, today)
            reports.extend(page_reports)
            break

    return _dedupe_reports(reports)


def _normalize_text(text: str) -> str:
    return re.sub(r"[\s\W_]+", "", text or "").lower()


def _strip_leading_byline(title: str) -> str:
    """
    Bondweb titles often start with an analyst/broker byline in parentheses,
    e.g. "(미래에셋증권 홍길동) ..." which should not be used for company matching.
    """
    stripped = (title or "").strip()
    while stripped.startswith("("):
        end = stripped.find(")")
        if end == -1:
            break
        stripped = stripped[end + 1 :].strip()
    return stripped


def _dedupe_reports(reports: list) -> list:
    deduped = []
    seen_urls = set()
    for report in reports:
        if report["report_url"] in seen_urls:
            continue
        seen_urls.add(report["report_url"])
        deduped.append(report)
    return deduped


def _encode_form_data(data: dict) -> bytes:
    body = "&".join(
        f"{key}={quote(str(value), encoding='euc-kr', safe='')}"
        for key, value in data.items()
    )
    return body.encode("ascii")


def _fetch_list_html(client: httpx.Client, data: dict) -> bytes:
    resp = client.post(LIST_URL, content=_encode_form_data(data))
    resp.raise_for_status()
    return resp.content


def _extract_list_cursors(html_bytes: bytes) -> tuple[str, str]:
    soup = BeautifulSoup(html_bytes.decode("euc-kr", errors="replace"), "lxml")
    values = [tag.get("value", "") for tag in soup.select('input[name="nTr"]')]
    if not values:
        return "0", "0"
    return values[0], values[-1]


def _title_likely_about_company(title: str, company: str) -> bool:
    stripped = _strip_leading_byline(title)
    normalized_title = _normalize_text(stripped)
    normalized_company = _normalize_text(company)
    if not normalized_title or not normalized_company:
        return False
    if normalized_company not in normalized_title:
        return False

    # Prefer titles that lead with the company name or clearly frame it as
    # the covered stock rather than just mentioning it in passing.
    title_no_space = re.sub(r"\s+", "", stripped)
    return (
        title_no_space.startswith(company)
        or f"({company}" in stripped
        or f"[{company}" in stripped
        or f"{company}(" in stripped
        or f"{company}/" in stripped
        or f"{company} -" in stripped
        or f"{company}:" in stripped
    )


def _search_reports_by_companies(
    company_map: dict[str, str],
    pages: int = 1,
    per_company_limit: int = 5,
) -> list:
    reports = []
    today = date.today()
    total_companies = len(company_map)

    with httpx.Client(timeout=30, headers=HEADERS) as client:
        for idx, (company, ticker) in enumerate(company_map.items(), start=1):
            print(
                f"[Bondweb] Searching {company} ({ticker}) "
                f"[{idx}/{total_companies}]"
            )
            company_reports = []
            params = {
                **LIST_PARAMS,
                "srhItem": "nIdSjt",
                "srhWord": company,
                "srhDate": "",
                "BoardLink": "",
                "HcCnt": "",
                "DATA_CYCLE": "",
                "tTime": str(int(time.time() * 1000)),
                "actNum": "0",
                "lstNumN": "0",
                "lstNumO": "0",
            }

            for page_idx in range(max(1, pages)):
                html_bytes = _fetch_list_html(client, params)
                page_reports = _parse_list(html_bytes, today)
                print(
                    f"    page {page_idx + 1}: {len(page_reports)} raw hits"
                )

                matched_any = False
                for report in page_reports:
                    if _title_likely_about_company(report["title"], company):
                        report["ticker"] = ticker
                        report["company"] = company
                        company_reports.append(report)
                        matched_any = True

                company_reports = _dedupe_reports(company_reports)
                print(
                    f"    matched so far: {len(company_reports)} "
                    f"(limit {per_company_limit})"
                )
                if len(company_reports) >= per_company_limit:
                    company_reports = company_reports[:per_company_limit]
                    break

                if page_idx == pages - 1:
                    break

                lst_num_n, lst_num_o = _extract_list_cursors(html_bytes)
                if lst_num_o in ("", "0") or lst_num_o == params["lstNumO"]:
                    break

                params["actNum"] = "2"
                params["lstNumN"] = lst_num_n
                params["lstNumO"] = lst_num_o

                if not matched_any and not page_reports:
                    break

            print(
                f"[Bondweb] Collected {len(company_reports[:per_company_limit])} "
                f"report(s) for {company}"
            )
            reports.extend(company_reports[:per_company_limit])

    return _dedupe_reports(reports)


def _parse_list(html_bytes: bytes, report_date: date) -> list:
    soup = BeautifulSoup(html_bytes.decode("euc-kr", errors="replace"), "lxml")
    reports = []

    for row in soup.select("tr"):
        cells = row.select("td")
        if len(cells) < 5:
            continue

        try:
            title = cells[1].get_text(strip=True)
            broker = cells[3].get_text(strip=True)

            if not title or not broker:
                continue

            # Extract report ID from javascript:getFileDown(894115, 1) — located in cell[4]
            dl_link = cells[4].select_one("a[href^='javascript:getFileDown']")
            if not dl_link:
                continue

            match = re.search(r"getFileDown\((\d+)", dl_link.get("href", ""))
            if not match:
                continue
            report_id = match.group(1)

            # Build a stable unique URL using the report ID
            report_url = f"{DOWNLOAD_URL}?number={report_id}&gn=1"

            reports.append({
                "ticker": "",        # unknown until Gemini extracts it
                "company": "",       # unknown until Gemini extracts it
                "broker": broker,
                "source": "bondweb",
                "title": title,
                "report_url": report_url,
                "report_date": report_date,
                "report_id": report_id,
            })
        except Exception:
            continue

    return reports


def _filter_by_whitelist(reports: list, ticker_whitelist: set) -> list:
    """
    Filters reports whose title mentions a KOSPI 200 company name.
    ticker_whitelist is a dict of {company_name: ticker} for name matching.
    Reports with no name match are DROPPED to avoid wasting Gemini API calls.
    """
    if not ticker_whitelist or not isinstance(ticker_whitelist, dict):
        return reports

    company_items = [
        (company, ticker, _normalize_text(company))
        for company, ticker in ticker_whitelist.items()
    ]
    # Prefer longer company names first so broad substrings do not steal matches.
    company_items.sort(key=lambda item: len(item[2]), reverse=True)

    matched = []
    for report in reports:
        title = report["title"]
        broker = report.get("broker", "")
        normalized_title = _normalize_text(_strip_leading_byline(title))
        normalized_broker = _normalize_text(broker)

        for company, ticker, normalized_company in company_items:
            if not normalized_company:
                continue
            if normalized_company == normalized_broker:
                continue
            if normalized_company in normalized_title:
                report["ticker"] = ticker
                report["company"] = company
                matched.append(report)
                break
        # No match → skip (don't waste a Gemini call on macro/bond reports)

    return matched


def download_pdf(report_url: str) -> Optional[bytes]:
    """Downloads a PDF from bondweb using its download URL."""
    # report_url is like: .../DownloadPage.asp?number=894115&gn=1
    # Convert to a POST request
    match = re.search(r"number=(\d+)&gn=(\d+)", report_url)
    if not match:
        return None

    number, gn = match.group(1), match.group(2)

    with httpx.Client(timeout=60, headers=HEADERS) as client:
        resp = client.post(DOWNLOAD_URL, data={"number": number, "gn": gn})
        resp.raise_for_status()
        if len(resp.content) < 1000:  # too small to be a real PDF
            return None
        return resp.content


if __name__ == "__main__":
    reports = fetch_recent_reports(pages=1)
    print(f"Found {len(reports)} reports")
    for r in reports[:5]:
        print(f"  [{r['report_id']}] {r['broker']} — {r['title'][:60]}")

    # Test PDF download
    if reports:
        print(f"\nDownloading: {reports[0]['report_url']}")
        pdf = download_pdf(reports[0]["report_url"])
        print(f"Downloaded {len(pdf):,} bytes" if pdf else "Failed")
