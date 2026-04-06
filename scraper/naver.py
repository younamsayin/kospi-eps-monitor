"""
Scrapes Naver Finance research page for new analyst reports.
URL: https://finance.naver.com/research/company_list.naver
"""

import httpx
from bs4 import BeautifulSoup
from datetime import date, datetime
from typing import Optional


NAVER_RESEARCH_URL = "https://finance.naver.com/research/company_list.naver"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://finance.naver.com/research/",
    "Accept-Language": "ko-KR,ko;q=0.9",
}


def fetch_recent_reports(pages: int = 3, ticker_whitelist: Optional[set] = None) -> list:
    """
    Fetches recent analyst reports from Naver Finance research.

    Args:
        pages: Number of listing pages to scrape (each page ~20 reports)
        ticker_whitelist: If provided, only return reports for these tickers (KOSPI 200)

    Returns:
        List of report dicts: {ticker, company, broker, title, report_url, report_date}
    """
    reports = []

    with httpx.Client(timeout=30, headers=HEADERS) as client:
        for page in range(1, pages + 1):
            params = {"pageSize": "20", "page": str(page)}
            resp = client.get(NAVER_RESEARCH_URL, params=params)
            resp.raise_for_status()

            page_reports = _parse_report_list(resp.text)
            reports.extend(page_reports)

            if not page_reports:
                break

    if ticker_whitelist:
        reports = [r for r in reports if r["ticker"] in ticker_whitelist]

    return reports


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

            # cell[3] has the direct PDF link
            pdf_tag = cells[3].select_one("a[href]")
            pdf_url = pdf_tag["href"] if pdf_tag else None

            # Fallback: use the report page URL from title link
            if not pdf_url:
                href = title_tag.get("href", "")
                pdf_url = "https://finance.naver.com/research/" + href if href else None

            broker = cells[2].get_text(strip=True)
            date_str = cells[4].get_text(strip=True)
            report_date = _parse_date(date_str)

            if ticker and pdf_url:
                reports.append(
                    {
                        "ticker": ticker,
                        "company": company,
                        "broker": broker,
                        "title": title,
                        "report_url": pdf_url,   # direct PDF URL
                        "report_date": report_date,
                    }
                )
        except Exception:
            continue

    return reports


def _parse_date(date_str: str) -> Optional[date]:
    for fmt in ("%y.%m.%d", "%Y.%m.%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None


def download_pdf(pdf_url: str) -> Optional[bytes]:
    """Downloads a PDF directly from its URL (as found in Naver's table cell[3])."""
    with httpx.Client(timeout=60, headers=HEADERS, follow_redirects=True) as client:
        resp = client.get(pdf_url)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "")
        if "text/html" in content_type:  # got an error page instead of PDF
            return None
        return resp.content


if __name__ == "__main__":
    reports = fetch_recent_reports(pages=1)
    print(f"Found {len(reports)} reports")
    for r in reports[:5]:
        print(r)
