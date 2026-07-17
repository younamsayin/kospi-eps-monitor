import unittest

from extractor.gemini import _normalize_extraction_payload, _normalize_report_date
from monitor import (
    _apply_extracted_metadata,
    _date_drift_days,
    _eps_matches_net_profit,
    _estimate_shift_score,
    _normalize_estimates,
    _relative_change,
    _values_agree,
)
from normalization import (
    canonical_broker,
    canonicalize_report_broker,
    normalize_recommendation,
)
from scraper.bondweb import _contains_company_token, _title_likely_about_company
from scraper.quote import parse_korean_amount


class HelperTests(unittest.TestCase):
    def test_normalize_report_date_handles_korean_format(self):
        self.assertEqual(_normalize_report_date("2026년 4월 6일"), "2026-04-06")

    def test_normalize_extraction_payload_rejects_multi_company_lists(self):
        payload = [
            {"ticker": "000660", "company": "SK하이닉스", "fiscal_year": 2026, "fwd_eps": 1},
            {"ticker": "005930", "company": "삼성전자", "fiscal_year": 2026, "fwd_eps": 2},
        ]
        self.assertIsNone(_normalize_extraction_payload(payload))

    def test_bondweb_company_boundary_rejects_broker_prefix_match(self):
        self.assertFalse(_contains_company_token("현대차증권리서치센터 - 모닝미팅자료", "현대차"))
        self.assertFalse(
            _title_likely_about_company("[현대차증권] 현대차증권리서치센터 - 모닝미팅자료", "현대차")
        )

    def test_bondweb_company_boundary_accepts_real_company_reference(self):
        self.assertTrue(_contains_company_token("자동차 - 현대차/기아 3월 글로벌 판매", "현대차"))
        self.assertTrue(_title_likely_about_company("[유진/이재일]자동차 - 현대차/기아 3월 글로벌 판매", "현대차"))

    def test_normalize_recommendation_maps_variants(self):
        self.assertEqual(normalize_recommendation("BUY"), "BUY")
        self.assertEqual(normalize_recommendation("매수"), "BUY")
        self.assertEqual(normalize_recommendation("Trading Buy"), "BUY")
        self.assertEqual(normalize_recommendation("Marketperform"), "HOLD")
        self.assertEqual(normalize_recommendation("중립"), "HOLD")
        self.assertEqual(normalize_recommendation("Not Rated"), "NOT_RATED")
        self.assertEqual(normalize_recommendation("NR"), "NOT_RATED")
        self.assertIsNone(normalize_recommendation("something else"))
        self.assertIsNone(normalize_recommendation(None))
        self.assertIsNone(normalize_recommendation("  "))

    def test_canonical_broker_maps_renames_and_preserves_raw(self):
        self.assertEqual(canonical_broker("하이투자증권"), "iM증권")
        self.assertEqual(canonical_broker("이베스트투자증권"), "LS증권")
        self.assertEqual(canonical_broker("삼성증권"), "삼성증권")
        self.assertIsNone(canonical_broker(None))

        report = {"broker": "하이투자증권"}
        canonicalize_report_broker(report)
        self.assertEqual(report["broker"], "iM증권")
        self.assertEqual(report["broker_raw"], "하이투자증권")

        unchanged = {"broker": "삼성증권"}
        canonicalize_report_broker(unchanged)
        self.assertEqual(unchanged["broker"], "삼성증권")
        self.assertNotIn("broker_raw", unchanged)

    def test_eps_matches_net_profit_is_unit_agnostic(self):
        # 5,000억 net profit / 100M shares -> EPS 5,000 (억원 table)
        self.assertTrue(_eps_matches_net_profit(5000, 5000, 100_000_000, 2.0))
        # Same report printed in 십억원: net_profit=500 -> still matches
        self.assertTrue(_eps_matches_net_profit(5000, 500, 100_000_000, 2.0))
        # 100x off under every unit interpretation -> mismatch
        self.assertFalse(_eps_matches_net_profit(5, 5000, 100_000_000, 2.0))
        # Not checkable without inputs
        self.assertIsNone(_eps_matches_net_profit(None, 500, 100_000_000, 2.0))
        self.assertIsNone(_eps_matches_net_profit(5000, 500, None, 2.0))

    def test_values_agree_and_relative_change(self):
        self.assertTrue(_values_agree(100_000, 100_500, 0.02))
        self.assertFalse(_values_agree(100_000, 150_000, 0.02))
        self.assertFalse(_values_agree(100_000, None, 0.02))
        self.assertAlmostEqual(_relative_change(150, 100), 0.5)
        self.assertIsNone(_relative_change(150, None))
        self.assertIsNone(_relative_change(150, 0))

    def test_report_date_drift_guard_keeps_scraper_date(self):
        report = {"ticker": "005930", "report_date": "2026-07-01"}
        _apply_extracted_metadata(report, {"report_date": "2026-01-05"})
        self.assertEqual(report["report_date"], "2026-07-01")
        self.assertIn("_date_drift", report)

        report = {"ticker": "005930", "report_date": "2026-07-01"}
        _apply_extracted_metadata(report, {"report_date": "2026-07-03"})
        self.assertEqual(report["report_date"], "2026-07-03")

        # No scraper date -> trust the extracted date
        report = {"ticker": "005930"}
        _apply_extracted_metadata(report, {"report_date": "2026-01-05"})
        self.assertEqual(report["report_date"], "2026-01-05")

        self.assertEqual(_date_drift_days("2026-07-01", "2026-07-08"), 7)
        self.assertIsNone(_date_drift_days(None, "2026-07-08"))

    def test_normalize_estimates_keeps_prior_fiscal_year_in_q1(self):
        # No broker/ticker so the prior-report lookup (and conn) is never used
        extracted = {"estimates": [
            {"fiscal_year": 2025, "fwd_eps": 100.0},
            {"fiscal_year": 2026, "fwd_eps": 120.0},
        ]}
        january_report = {"report_date": "2026-02-15"}
        kept = _normalize_estimates(None, january_report, dict(extracted))
        self.assertEqual([est["fiscal_year"] for est in kept], [2025, 2026])

        july_report = {"report_date": "2026-07-15"}
        kept = _normalize_estimates(None, july_report, dict(extracted))
        self.assertEqual([est["fiscal_year"] for est in kept], [2026])

    def test_parse_korean_amount(self):
        self.assertEqual(parse_korean_amount("1,490조 8,010억"), 1490e12 + 8010e8)
        self.assertEqual(parse_korean_amount("5,000억"), 5000e8)
        self.assertIsNone(parse_korean_amount(""))
        self.assertIsNone(parse_korean_amount(None))

    def test_estimate_shift_score_prefers_shift_when_series_is_left_shifted(self):
        current_map = {2025: 27182.0, 2026: 58955.0, 2027: 274331.0, 2028: 392853.0}
        previous_map = {2024: 27182.0, 2025: 58955.0, 2026: 274331.0, 2027: 392853.0}

        no_shift_score, no_shift_matches = _estimate_shift_score(current_map, previous_map, 0)
        shift_down_score, shift_down_matches = _estimate_shift_score(current_map, previous_map, -1)

        self.assertGreater(shift_down_matches, no_shift_matches)
        self.assertGreater(shift_down_score, no_shift_score)


if __name__ == "__main__":
    unittest.main()
