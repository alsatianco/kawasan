import unittest
from results import validate

class ResultGateTest(unittest.TestCase):
    def test_empty_duplicate_missing_and_skip_are_not_passes(self):
        for rows in ([], [{"case": "produce-consume", "status": "SKIP"}],
                     [{"case": "produce-consume", "status": "PASS"}] * 2):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                validate("python", "kafka", rows, [])

    def test_oracle_cannot_use_allowlist(self):
        rows = [{"case": "produce-consume", "status": "FAIL", "error": "broken"}]
        with self.assertRaises(ValueError):
            validate("python", "kafka", rows, [("python", "produce-consume", "broken", "CM-3", "reason")])

    def test_allowlist_requires_matching_error_and_no_unexpected_pass(self):
        allow = [("python", "produce-consume", "broken", "CM-3", "reason")]
        validate("python", "kawasan", [{"case": "produce-consume", "status": "FAIL", "error": "broken"}], allow)
        for row in ({"case": "produce-consume", "status": "FAIL", "error": "different"},
                    {"case": "produce-consume", "status": "PASS"}):
            with self.subTest(row=row), self.assertRaises(ValueError):
                validate("python", "kawasan", [row], allow)

    def test_unknown_case_and_allowlist_duplicates_are_rejected(self):
        with self.assertRaises(ValueError):
            validate("python", "kafka", [{"case": "unknown", "status": "PASS"}], [])
        allow = [("python", "produce-consume", "broken", "CM-3", "reason")] * 2
        with self.assertRaises(ValueError):
            validate("python", "kawasan", [{"case": "produce-consume", "status": "FAIL", "error": "broken"}], allow)

if __name__ == "__main__":
    unittest.main()
