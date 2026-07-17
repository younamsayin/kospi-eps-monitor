"""
Interactive helper for hand-verifying golden-set entries.

For each unverified entry it opens the PDF (macOS Preview) and shows the
extractor's values. You confirm them, correct them, or skip. Progress is
saved back to tests/golden/golden_set.json after every answer, so you can
stop with 'q' at any time and resume later.

Usage:
    python3 scripts/verify_golden_set.py
"""

import json
import os
import subprocess
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GOLDEN_PATH = os.path.join(PROJECT_ROOT, "tests", "golden", "golden_set.json")


def _save(golden: dict):
    with open(GOLDEN_PATH, "w", encoding="utf-8") as f:
        json.dump(golden, f, ensure_ascii=False, indent=2)


def _prompt_number(label: str, current):
    """Return the corrected number, or the current value if input is empty."""
    while True:
        raw = input(f"    {label} [{current}]: ").strip().replace(",", "")
        if raw == "":
            return current
        if raw.lower() in ("null", "none", "-"):
            return None
        try:
            value = float(raw)
            return int(value) if value == int(value) else value
        except ValueError:
            print("    Enter a number, blank to keep, or 'null' to clear.")


def main():
    with open(GOLDEN_PATH, encoding="utf-8") as f:
        golden = json.load(f)

    entries = golden.get("entries", [])
    pending = [e for e in entries if not e.get("verified")]
    done = len(entries) - len(pending)
    print(f"{len(entries)} entries total, {done} verified, {len(pending)} to go.")
    print("Keys: y = values are correct | e = edit values | s = skip | q = quit (progress is saved)\n")

    for entry in entries:
        if entry.get("verified"):
            continue
        pdf_path = entry["pdf_path"]
        if not os.path.isabs(pdf_path):
            pdf_path = os.path.join(PROJECT_ROOT, pdf_path)
        if not os.path.exists(pdf_path):
            print(f"! PDF missing, skipping: {pdf_path}")
            continue

        expected = entry["expected"]
        print("=" * 72)
        print(f"{entry.get('company')} ({expected.get('ticker')}) — {entry.get('broker')} — {entry.get('report_date')}")
        print(f"Title: {entry.get('title')}")
        print(f"  target_price: {expected.get('target_price')}")
        for fy, eps in sorted((expected.get("estimates") or {}).items()):
            print(f"  EPS FY{fy}:   {eps}")
        subprocess.run(["open", pdf_path], check=False)

        answer = ""
        while answer not in ("y", "e", "s", "q"):
            answer = input("Correct? [y/e/s/q]: ").strip().lower()

        if answer == "q":
            break
        if answer == "s":
            continue
        if answer == "e":
            print("  Blank keeps the shown value; 'null' clears it.")
            expected["target_price"] = _prompt_number("target_price", expected.get("target_price"))
            estimates = expected.get("estimates") or {}
            for fy in sorted(estimates):
                estimates[fy] = _prompt_number(f"EPS FY{fy}", estimates[fy])
            expected["estimates"] = {fy: v for fy, v in estimates.items() if v is not None}
            ticker = input(f"    ticker [{expected.get('ticker')}]: ").strip()
            if ticker:
                expected["ticker"] = ticker
        entry["verified"] = True
        _save(golden)
        remaining = sum(1 for e in entries if not e.get("verified"))
        print(f"Saved. {remaining} entries remaining.\n")

    verified = sum(1 for e in entries if e.get("verified"))
    print(f"\n{verified}/{len(entries)} entries verified. "
          f"Run: python3 scripts/eval_extraction.py")


if __name__ == "__main__":
    main()
