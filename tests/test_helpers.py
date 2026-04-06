import unittest

from extractor.gemini import _normalize_extraction_payload, _normalize_report_date
from monitor import _estimate_shift_score
from scraper.bondweb import _contains_company_token, _title_likely_about_company


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

    def test_estimate_shift_score_prefers_shift_when_series_is_left_shifted(self):
        current_map = {2025: 27182.0, 2026: 58955.0, 2027: 274331.0, 2028: 392853.0}
        previous_map = {2024: 27182.0, 2025: 58955.0, 2026: 274331.0, 2027: 392853.0}

        no_shift_score, no_shift_matches = _estimate_shift_score(current_map, previous_map, 0)
        shift_down_score, shift_down_matches = _estimate_shift_score(current_map, previous_map, -1)

        self.assertGreater(shift_down_matches, no_shift_matches)
        self.assertGreater(shift_down_score, no_shift_score)


if __name__ == "__main__":
    unittest.main()
