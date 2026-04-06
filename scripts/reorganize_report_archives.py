import argparse
import os
import re
import shutil
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = Path(os.environ.get("REPORTS_DIR", REPO_ROOT / "reports"))


def _is_date_dir(name: str) -> bool:
    return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", name))


def _parse_filename(name: str):
    if not name.lower().endswith(".pdf"):
        return None
    stem = name[:-4]
    parts = stem.split("_", 3)
    if len(parts) != 4:
        return None
    ticker, company, broker, title = parts
    return {
        "ticker": ticker,
        "company": company or "unknown",
        "broker": broker,
        "title": title,
    }


def _iter_old_layout_files(root: Path):
    for date_dir in root.iterdir():
        if not date_dir.is_dir() or not _is_date_dir(date_dir.name):
            continue
        for source_dir in date_dir.iterdir():
            if not source_dir.is_dir():
                continue
            for pdf_path in source_dir.glob("*.pdf"):
                yield date_dir.name, source_dir.name, pdf_path


def main():
    parser = argparse.ArgumentParser(description="Reorganize reports/ from date/source/file to company/source/date/file.")
    parser.add_argument("--apply", action="store_true", help="Move files instead of only printing what would change.")
    parser.add_argument("--limit", type=int, default=20, help="How many sample moves to print.")
    args = parser.parse_args()

    if not REPORTS_DIR.exists():
        print(f"Reports directory does not exist: {REPORTS_DIR}")
        return

    moves = []
    skipped = 0
    for report_date, source, pdf_path in _iter_old_layout_files(REPORTS_DIR):
        parsed = _parse_filename(pdf_path.name)
        if not parsed:
            skipped += 1
            continue
        target = REPORTS_DIR / parsed["company"] / source / report_date / pdf_path.name
        if pdf_path == target:
            continue
        moves.append((pdf_path, target))

    print(f"Found {len(moves)} file(s) to move. skipped_unparsed={skipped}")
    for src, dst in moves[: args.limit]:
        print(f"{src} -> {dst}")
    if len(moves) > args.limit:
        print(f"... and {len(moves) - args.limit} more")

    if not args.apply:
        return

    moved = 0
    for src, dst in moves:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            src.unlink()
        else:
            shutil.move(str(src), str(dst))
        moved += 1

    # Remove empty old date/source directories.
    for date_dir in sorted(REPORTS_DIR.iterdir(), reverse=True):
        if not date_dir.is_dir() or not _is_date_dir(date_dir.name):
            continue
        for source_dir in sorted(date_dir.iterdir(), reverse=True):
            if source_dir.is_dir():
                try:
                    source_dir.rmdir()
                except OSError:
                    pass
        try:
            date_dir.rmdir()
        except OSError:
            pass

    print(f"Moved {moved} file(s).")


if __name__ == "__main__":
    main()
