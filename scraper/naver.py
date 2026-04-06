"""
Scrapes Naver Finance research page for new analyst reports.
URL: https://finance.naver.com/research/company_list.naver
"""

import os
import time
import logging
import httpx
import json
import re
from bs4 import BeautifulSoup
from datetime import date, datetime
from typing import Optional
from urllib.parse import urljoin, urlparse


NAVER_RESEARCH_URL = "https://finance.naver.com/research/company_list.naver"
NAVER_RESEARCH_BASE_URL = "https://finance.naver.com/research/"
DEFAULT_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

HEADERS = {
    "User-Agent": os.environ.get("SCRAPER_USER_AGENT", DEFAULT_USER_AGENT),
    "Referer": "https://finance.naver.com/research/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}
logger = logging.getLogger(__name__)
MAX_COMPANY_SEARCH_PAGES = 1
MAX_COMPANY_REPORTS = 10


def _request_with_retry(client: httpx.Client, method: str, url: str, **kwargs) -> httpx.Response:
    last_exc = None
    for attempt in range(3):
        try:
            resp = client.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except httpx.HTTPError as exc:
            last_exc = exc
            if attempt == 2:
                raise
            delay = 2 ** attempt
            logger.warning("Naver request failed (%s %s): %s. Retrying in %ss", method, url, exc, delay)
            time.sleep(delay)
    raise last_exc


def fetch_recent_reports(pages: int = 3, ticker_whitelist: Optional[set] = None) -> list:
    """
    Fetches recent analyst reports from Naver Finance research.

    Args:
        pages: Number of listing pages to scrape (each page ~20 reports)
        ticker_whitelist: If provided, only return reports for these tickers (KOSPI 200)

    Returns:
        List of report dicts: {ticker, company, broker, title, report_url, report_date}
    """
    if ticker_whitelist:
        return _fetch_reports_by_ticker_search(ticker_whitelist, pages=min(max(1, pages), MAX_COMPANY_SEARCH_PAGES))

    reports = []
    with httpx.Client(timeout=30, headers=HEADERS) as client:
        for page in range(1, pages + 1):
            params = {"pageSize": "20", "page": str(page)}
            resp = _request_with_retry(client, "GET", NAVER_RESEARCH_URL, params=params)

            page_reports = _parse_report_list(resp.text)
            reports.extend(page_reports)

            if not page_reports:
                break

    return _dedupe_reports(reports)


def _fetch_reports_by_ticker_search(ticker_whitelist: set, pages: int) -> list:
    reports = []
    tickers = sorted(str(ticker).zfill(6) for ticker in ticker_whitelist)
    total_tickers = len(tickers)

    with httpx.Client(timeout=30, headers=HEADERS) as client:
        for idx, ticker in enumerate(tickers, start=1):
            print(f"[Naver] Searching ticker {ticker} [{idx}/{total_tickers}]")
            ticker_reports = []
            for page in range(1, min(pages, MAX_COMPANY_SEARCH_PAGES) + 1):
                params = {
                    "searchType": "itemCode",
                    "itemCode": ticker,
                    "page": str(page),
                }
                resp = _request_with_retry(client, "GET", NAVER_RESEARCH_URL, params=params)
                page_reports = _parse_report_list(resp.text)
                ticker_reports.extend(page_reports)
                print(f"    page {page}: {len(page_reports)} raw hits")
                if not page_reports:
                    break
            ticker_reports = _dedupe_reports(ticker_reports)
            ticker_reports = ticker_reports[:MAX_COMPANY_REPORTS]
            print(f"[Naver] Collected {len(ticker_reports)} report(s) for {ticker}")
            reports.extend(ticker_reports)

    return _dedupe_reports(reports)


def _dedupe_reports(reports: list) -> list:
    deduped = []
    seen_urls = set()
    for report in reports:
        report_url = report.get("report_url")
        if not report_url or report_url in seen_urls:
            continue
        seen_urls.add(report_url)
        deduped.append(report)
    return deduped


def _parse_report_list(html: str) -> list:
    soup = BeautifulSoup(html, "lxml")
    reports = []

    table = soup.select_one("table.type_1")
    if not table:
        return reports

    for row in table.select("tr"):
        cells = row.select("td")
        if len(cells) < 5:
            continue

        # Naver research table columns:
        # 0: company + ticker link, 1: report title, 2: broker, 3: PDF download link, 4: date
        try:
            company_tag = cells[0].select_one("a")
            if not company_tag:
                continue

            company = company_tag.get_text(strip=True)
            company_href = company_tag.get("href", "")
            ticker = ""
            if "code=" in company_href:
                ticker = company_href.split("code=")[-1].strip()

            title_tag = cells[1].select_one("a")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)

            report_page_url = urljoin(NAVER_RESEARCH_BASE_URL, title_tag.get("href", ""))

            # cell[3] usually has the direct PDF link
            pdf_tag = cells[3].select_one("a[href]")
            pdf_url = pdf_tag["href"] if pdf_tag else None

            # Keep the report page URL too so download_pdf can resolve cases
            # where Naver no longer exposes a direct PDF link in the list row.
            if pdf_url:
                pdf_url = urljoin(NAVER_RESEARCH_BASE_URL, pdf_url)

            broker = cells[2].get_text(strip=True)
            date_str = cells[4].get_text(strip=True)
            report_date = _parse_date(date_str)

            if ticker:
                reports.append(
                    {
                        "ticker": ticker,
                        "company": company,
                        "broker": broker,
                        "source": "naver",
                        "title": title,
                        "report_url": pdf_url or report_page_url,
                        "report_page_url": report_page_url,
                        "report_date": report_date,
                    }
                )
        except Exception as exc:
            logger.warning("Failed to parse Naver report row: %s", exc)
            continue

    return reports


def _parse_date(date_str: str) -> Optional[date]:
    for fmt in ("%y.%m.%d", "%Y.%m.%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _extract_pdf_url_from_report_page(html: str, page_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")

    # Some detail pages still embed a direct PDF link.
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full_url = urljoin(page_url, href)
        if ".pdf" in full_url.lower() or "stock.pstatic.net" in full_url.lower():
            return full_url

    # Fallback: look for PDF-like URLs inside scripts or inline HTML.
    match = re.search(r'https?://[^"\']+\.pdf', html, re.IGNORECASE)
    if match:
        return match.group(0)

    return None


def _extract_external_report_url_from_page(html: str, page_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full_url = urljoin(page_url, href)
        parsed = urlparse(full_url)
        if parsed.netloc and "finance.naver.com" not in parsed.netloc:
            return full_url
    return None


def _find_pdf_like_link_in_html(html: str, page_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full_url = urljoin(page_url, href)
        lower_url = full_url.lower()
        label = a.get_text(" ", strip=True).lower()
        if ".pdf" in lower_url:
            return full_url
        if any(token in lower_url for token in ["download", "file", "attachment", "attach"]):
            return full_url
        if any(token in label for token in ["pdf", "다운로드", "첨부", "원문"]):
            return full_url

    match = re.search(r'https?://[^"\']+\.pdf', html, re.IGNORECASE)
    if match:
        return match.group(0)
    return None


def _resolve_shinhan_pdf_url(client: httpx.Client, external_url: str, report: Optional[dict]) -> Optional[str]:
    """
    Best-effort Shinhan resolver.
    Some Naver detail pages only link to Shinhan's research popup. We try the
    open search endpoint first and build a direct candidate from the returned
    metadata when it looks trustworthy.
    """
    search_url = "https://www.shinhansec.com/siw/etc/browse/search05/data.do"
    queries = []
    if report:
        title = (report.get("title") or "").strip()
        company = (report.get("company") or "").strip()
        if title:
            queries.append(title)
            queries.extend([part.strip() for part in re.split(r"[:;,\\-]", title) if part.strip()])
        if company:
            queries.append(company)

    seen = set()
    for query in queries:
        if query in seen:
            continue
        seen.add(query)
        try:
            resp = _request_with_retry(
                client,
                "POST",
                search_url,
                data={"startCount": 0, "listCount": 10, "query": query, "searchType": "A", "boardCode": ""},
                headers={"Referer": external_url},
            )
            body = resp.json().get("body", {})
            collections = body.get("collectionList", [])
            if not collections:
                continue
            for item in collections[0].get("itemList", []):
                if str(item.get("EXT", "")).lower() != "pdf":
                    continue
                file_path = item.get("FILE_PATH")
                display_name = item.get("DISPLAYNAME")
                title = str(item.get("TITLE", ""))
                company = str(report.get("company", "")) if report else ""
                if not file_path or not display_name:
                    continue
                # Require at least some textual overlap so we do not grab a
                # random report from Shinhan's recent feed.
                if company and company not in title and company not in query:
                    continue
                return urljoin("https://www.shinhansec.com", f"{file_path}/{display_name}")
        except (httpx.HTTPError, json.JSONDecodeError):
            continue

    return None


def _resolve_external_pdf_url(client: httpx.Client, external_url: str, report: Optional[dict]) -> Optional[str]:
    parsed = urlparse(external_url)
    host = parsed.netloc.lower()

    if "shinhansec.com" in host:
        resolved = _resolve_shinhan_pdf_url(client, external_url, report)
        if resolved:
            return resolved

    try:
        resp = _request_with_retry(client, "GET", external_url)
    except httpx.HTTPError:
        return None

    return _find_pdf_like_link_in_html(resp.text, str(resp.url))


def download_pdf(pdf_url: str, report: Optional[dict] = None) -> Optional[bytes]:
    """
    Downloads a PDF from Naver research.
    If given a report detail page, first resolve the actual PDF URL from that page.
    """
    with httpx.Client(timeout=60, headers=HEADERS, follow_redirects=True) as client:
        target_url = pdf_url
        if "company_read.naver" in pdf_url:
            page_resp = _request_with_retry(client, "GET", pdf_url)
            resolved_pdf_url = _extract_pdf_url_from_report_page(page_resp.text, str(page_resp.url))
            if resolved_pdf_url:
                target_url = resolved_pdf_url
            else:
                external_url = _extract_external_report_url_from_page(page_resp.text, str(page_resp.url))
                if not external_url:
                    return None
                resolved_external_pdf_url = _resolve_external_pdf_url(client, external_url, report)
                if not resolved_external_pdf_url:
                    return None
                target_url = resolved_external_pdf_url

        resp = _request_with_retry(client, "GET", target_url)
        content_type = resp.headers.get("content-type", "")
        if "text/html" in content_type:  # got an error page instead of PDF
            return None
        return resp.content


if __name__ == "__main__":
    reports = fetch_recent_reports(pages=1)
    print(f"Found {len(reports)} reports")
    for r in reports[:5]:
        print(r)
