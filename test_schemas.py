import unittest

from pydantic import ValidationError

from schemas import ClickEvent, PaymentEvent


class SchemaTests(unittest.TestCase):
    def test_rejects_empty_click_id(self):
        for clid in ("", "   ", "\t\n"):
            with self.subTest(clid=repr(clid)):
                with self.assertRaises(ValidationError):
                    PaymentEvent(
                        clid=clid,
                        payout="10.00",
                        ts="2026-09-13T10:00:00+03:00",
                    )

    def test_rejects_non_finite_money(self):
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value):
                with self.assertRaises(ValidationError):
                    PaymentEvent(
                        clid="test",
                        payout=value,
                        ts="2026-09-13T10:00:00+03:00",
                    )

                with self.assertRaises(ValidationError):
                    ClickEvent(
                        clid="test",
                        ad_id=1,
                        click_spend=value,
                        ts="2026-09-13T10:00:00+03:00",
                    )

    def test_rejects_invalid_ad_id(self):
        for value in (True, 1.5, "17", 2**63, -(2**63) - 1):
            with self.subTest(value=value):
                with self.assertRaises(ValidationError):
                    ClickEvent(
                        clid="test",
                        ad_id=value,
                        click_spend="1.25",
                        ts="2026-09-13T10:00:00+03:00",
                    )

    def test_rejects_time_without_timezone(self):
        with self.assertRaises(ValidationError):
            PaymentEvent(
                clid="test",
                payout="10.00",
                ts="2026-09-13T10:00:00",
            )

    def test_equivalent_timezones_represent_same_moment(self):
        moscow = PaymentEvent(
            clid="test",
            payout="10.00",
            ts="2026-09-13T13:00:00+03:00",
        )
        utc = PaymentEvent(
            clid="test",
            payout="10.00",
            ts="2026-09-13T10:00:00Z",
        )
        self.assertEqual(moscow.ts, utc.ts)


if __name__ == "__main__":
    unittest.main()