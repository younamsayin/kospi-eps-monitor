"""
Uses Gemini API to extract FWD EPS and related data from analyst report PDFs.
Gemini supports native PDF understanding — no pre-parsing needed.
"""

import os
import json
import tempfile
from typing import Optional
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

MODEL = "gemini-2.5-flash"

EXTRACTION_PROMPT = """
You are a financial analyst assistant. This is a Korean stock analyst report (증권사 리포트).

Extract the following information and return ONLY valid JSON, no markdown, no explanation:

{
  "company": "company name in Korean",
  "ticker": "6-digit KRX ticker code if mentioned",
  "broker": "securities firm that wrote the report",
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
- EPS (주당순이익 or EPS) should be in KRW per share
- If a value is not found, use null
- fiscal_year must be an integer (e.g. 2025, 2026)
- Return only the JSON object, nothing else
"""


def _get_client() -> genai.Client:
    return genai.Client(api_key=os.environ["GEMINI_API_KEY"])


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

        return json.loads(raw)

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
