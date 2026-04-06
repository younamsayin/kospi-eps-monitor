"""
Uses Gemini API to extract FWD EPS and related data from analyst report PDFs.
Gemini supports native PDF understanding — no pre-parsing needed.
"""

import os
import json
import re
import tempfile
from datetime import datetime
from typing import Optional
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.1-flash-lite-preview")

EXTRACTION_PROMPT = """
You are a financial analyst assistant. This is a Korean stock analyst report (증권사 리포트).

Extract the following information and return ONLY valid JSON, no markdown, no explanation:

{
  "company": "company name in Korean",
  "ticker": "6-digit KRX ticker code if mentioned",
  "broker": "securities firm that wrote the report",
  "report_date": "publication date written in the report, formatted as YYYY-MM-DD, or null",
  "recommendation": "BUY / HOLD / SELL or Korean equivalent",
  "target_price": <integer, target price in KRW, or null>,
  "estimates": [
    {
      "fiscal_year": <integer, e.g. 2025>,
      "fwd_eps": <number, EPS estimate in KRW, or null>,
      "revenue": <number, revenue in 억원 (100M KRW), or null>,
      "operating_profit": <number, 영업이익 in 억원, or null>,
      "net_profit": <number, 순이익 in 억원, or null>
    }
  ]
}

Rules:
- Include estimates for all fiscal years mentioned (typically current year + 1-2 forward years)
- Focus on forward estimates (F, E, 전망치) rather than historical actuals when the report distinguishes them
- report_date should be the actual publication date written in the PDF, not today's date
- EPS (주당순이익 or EPS) should be in KRW per share
- If a value is not found, use null
- fiscal_year must be an integer (e.g. 2025, 2026)
- Do not shift fiscal years left or right; preserve the fiscal year labels exactly as shown in the report
- Return only the JSON object, nothing else
"""


def _get_client() -> genai.Client:
    return genai.Client(api_key=os.environ["GEMINI_API_KEY"])


def _normalize_report_date(value) -> Optional[str]:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        value = str(value)

    value = value.strip()
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%y.%m.%d", "%y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue

    match = re.search(r"(\d{4})\s*[./년-]\s*(\d{1,2})\s*[./월-]\s*(\d{1,2})", value)
    if match:
        year, month, day = (int(part) for part in match.groups())
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            return None

    return None


def _normalize_extraction_payload(payload) -> Optional[dict]:
    """Coerce Gemini JSON output into the dict shape expected by the monitor."""
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), dict):
            payload = payload["data"]
        elif isinstance(payload.get("result"), dict):
            payload = payload["result"]
    elif isinstance(payload, list):
        # Gemini occasionally returns a one-item JSON array even when asked
        # for a single object.
        dict_items = [item for item in payload if isinstance(item, dict)]
        if len(dict_items) == 1:
            payload = dict_items[0]
        elif dict_items:
            payload = {
                "company": dict_items[0].get("company", ""),
                "ticker": dict_items[0].get("ticker", ""),
                "broker": dict_items[0].get("broker", ""),
                "report_date": dict_items[0].get("report_date"),
                "recommendation": dict_items[0].get("recommendation"),
                "target_price": dict_items[0].get("target_price"),
                "estimates": dict_items,
            }
        else:
            return None
    else:
        return None

    payload.setdefault("company", "")
    payload.setdefault("ticker", "")
    payload.setdefault("broker", "")
    payload["report_date"] = _normalize_report_date(payload.get("report_date"))
    payload.setdefault("recommendation", None)
    payload.setdefault("target_price", None)

    estimates = payload.get("estimates")
    if isinstance(estimates, dict):
        payload["estimates"] = [estimates]
    elif not isinstance(estimates, list):
        payload["estimates"] = []

    return payload


def extract_eps_from_pdf(pdf_bytes: bytes) -> Optional[dict]:
    """
    Sends PDF bytes to Gemini and extracts structured EPS data.
    Returns parsed dict or None on failure.
    """
    try:
        client = _get_client()

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = tmp.name

        uploaded = client.files.upload(
            file=tmp_path,
            config=types.UploadFileConfig(mime_type="application/pdf"),
        )
        os.unlink(tmp_path)

        response = client.models.generate_content(
            model=MODEL,
            contents=[uploaded, EXTRACTION_PROMPT],
            config=types.GenerateContentConfig(
                temperature=0,
                response_mime_type="application/json",
            ),
        )

        raw = response.text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        parsed = json.loads(raw)
        normalized = _normalize_extraction_payload(parsed)
        if not normalized:
            print(f"[Gemini] Unexpected JSON shape: {type(parsed).__name__}")
            return None

        return normalized

    except Exception as e:
        print(f"[Gemini] Extraction failed: {e}")
        return None


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python gemini.py <path_to_pdf>")
        sys.exit(1)

    with open(sys.argv[1], "rb") as f:
        pdf_bytes = f.read()

    result = extract_eps_from_pdf(pdf_bytes)
    print(json.dumps(result, ensure_ascii=False, indent=2))
