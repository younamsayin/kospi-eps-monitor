"""
Fetches KOSPI 200 constituent list from Naver Finance.
(KRX data portal requires login; Naver is a reliable free alternative.)
"""

import os
import time
import logging
import httpx
from bs4 import BeautifulSoup

NAVER_KOSPI200_URL = "https://finance.naver.com/sise/entryJongmok.naver"
DEFAULT_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
HEADERS = {
    "User-Agent": os.environ.get("SCRAPER_USER_AGENT", DEFAULT_USER_AGENT),
    "Accept-Language": "ko-KR,ko;q=0.9",
}
TOTAL_PAGES = 20  # 10 stocks/page × 20 pages = 200
logger = logging.getLogger(__name__)


def _request_with_retry(client: httpx.Client, url: str, **kwargs) -> httpx.Response:
    last_exc = None
    for attempt in range(3):
        try:
            resp = client.get(url, **kwargs)
            resp.raise_for_status()
            return resp
        except httpx.HTTPError as exc:
            last_exc = exc
            if attempt == 2:
                raise
            delay = 2 ** attempt
            logger.warning("KRX fetch failed (%s). Retrying in %ss", exc, delay)
            time.sleep(delay)
    raise last_exc


def fetch_kospi200() -> list[dict]:
    """
    Returns list of {ticker, company} for current KOSPI 200 constituents.
    Scrapes Naver Finance index constituent pages.
    """
    results = []

    with httpx.Client(timeout=30, headers=HEADERS) as client:
        for page in range(1, TOTAL_PAGES + 1):
            resp = _request_with_retry(
                client,
                NAVER_KOSPI200_URL,
                params={"code": "KPI200", "page": str(page)},
            )

            soup = BeautifulSoup(resp.content.decode("euc-kr", errors="replace"), "lxml")
            links = soup.select('a[href*="/item/main.naver?code="]')

            for a in links:
                ticker = a["href"].split("code=")[-1].strip()
                company = a.get_text(strip=True)
                if ticker and company:
                    results.append({"ticker": ticker, "company": company})

    # Deduplicate (navigation links can repeat)
    seen = set()
    unique = []
    for r in results:
        if r["ticker"] not in seen:
            seen.add(r["ticker"])
            unique.append(r)

    return unique


if __name__ == "__main__":
    constituents = fetch_kospi200()
    print(f"Fetched {len(constituents)} KOSPI 200 constituents")
    for c in constituents[:10]:
        print(c)
