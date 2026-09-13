import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from database import open_connection
from repository import (
    claim_payment,
    insert_click,
    insert_payment,
    mark_delivered,
    schedule_retry,
)
from schemas import ClickEvent, PaymentEvent


class RepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.connections = []
        self.schema = "test_" + uuid4().hex

        self.connection = await open_connection()
        self.connections.append(self.connection)
        self.addAsyncCleanup(self.cleanup_database)

        await self.connection.execute(
            f'CREATE SCHEMA "{self.schema}"'
        )
        await self.connection.execute(
            f'SET search_path TO "{self.schema}"'
        )

        schema_sql = (
            Path(__file__).resolve().parent / "schema.sql"
        ).read_text(encoding="utf-8-sig")
        await self.connection.execute(schema_sql)

        self.time = datetime(2026, 9, 13, 10, tzinfo=timezone.utc)

    async def cleanup_database(self):
        for connection in self.connections[1:]:
            if not connection.is_closed():
                await connection.close()

        if not self.connection.is_closed():
            try:
                await self.connection.execute(
                    f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE'
                )
            finally:
                await self.connection.close()

    async def extra_connection(self):
        connection = await open_connection()
        self.connections.append(connection)
        await connection.execute(
            f'SET search_path TO "{self.schema}"'
        )
        return connection

    def click(self, clid="test-click", cost="1.25"):
        return ClickEvent(
            clid=clid,
            ad_id=17,
            click_spend=Decimal(cost),
            ts=self.time,
        )

    def payment(self, clid="test-click", minutes=5, payout="30.00"):
        return PaymentEvent(
            clid=clid,
            payout=Decimal(payout),
            ts=self.time + timedelta(minutes=minutes),
        )

    async def seed_ready_payment(self):
        await insert_click(self.connection, self.click())
        await insert_payment(self.connection, self.payment())

    async def test_duplicates_preserve_original_values(self):
        self.assertTrue(
            await insert_click(self.connection, self.click())
        )
        self.assertFalse(
            await insert_click(self.connection, self.click(cost="99"))
        )

        self.assertTrue(
            await insert_payment(self.connection, self.payment())
        )
        self.assertFalse(
            await insert_payment(
                self.connection, self.payment(payout="999")
            )
        )

        cost = await self.connection.fetchval(
            "SELECT click_spend FROM clicks"
        )
        payout = await self.connection.fetchval(
            "SELECT payout FROM payments"
        )
        self.assertEqual(cost, Decimal("1.25"))
        self.assertEqual(payout, Decimal("30.00"))

    async def test_payment_waits_for_click(self):
        await insert_payment(self.connection, self.payment())

        self.assertIsNone(
            await claim_payment(self.connection, 60)
        )

        await insert_click(self.connection, self.click())
        task = await claim_payment(self.connection, 60)

        self.assertIsNotNone(task)
        self.assertEqual(task["payout"], Decimal("30.00"))
        self.assertEqual(task["click_spend"], Decimal("1.25"))

    async def test_two_workers_get_different_payments(self):
        await self.seed_ready_payment()
        await insert_payment(
            self.connection,
            self.payment(minutes=10, payout="50"),
        )
        second = await self.extra_connection()

        # Удерживаем первую блокировку, пока второй worker выбирает задачу.
        transaction = self.connection.transaction()
        await transaction.start()

        try:
            first_task = await claim_payment(self.connection, 60)
            second_task = await asyncio.wait_for(
                claim_payment(second, 60),
                timeout=3,
            )

            self.assertIsNotNone(first_task)
            self.assertIsNotNone(second_task)
            self.assertNotEqual(
                (first_task["clid"], first_task["ts"]),
                (second_task["clid"], second_task["ts"]),
            )
        finally:
            await transaction.rollback()

    async def test_expired_lease_rejects_old_token(self):
        await self.seed_ready_payment()
        old_task = await claim_payment(self.connection, 60)

        await self.connection.execute(
            """
            UPDATE payments
            SET lease_until = clock_timestamp() - INTERVAL '1 second'
            WHERE clid = $1 AND ts = $2
            """,
            old_task["clid"],
            old_task["ts"],
        )

        new_task = await claim_payment(self.connection, 60)
        self.assertNotEqual(
            old_task["claim_token"], new_task["claim_token"]
        )
        self.assertEqual(new_task["attempt_count"], 2)

        old_result = await mark_delivered(
            self.connection,
            old_task["clid"],
            old_task["ts"],
            old_task["claim_token"],
        )
        self.assertFalse(old_result)

        new_result = await mark_delivered(
            self.connection,
            new_task["clid"],
            new_task["ts"],
            new_task["claim_token"],
        )
        self.assertTrue(new_result)
        self.assertIsNone(await claim_payment(self.connection, 60))

    async def test_retry_is_persisted_and_respects_delay(self):
        await self.seed_ready_payment()
        task = await claim_payment(self.connection, 60)

        updated = await schedule_retry(
            self.connection,
            task["clid"],
            task["ts"],
            task["claim_token"],
            "HTTP 503",
            3600,
        )
        self.assertTrue(updated)

        second = await self.extra_connection()
        row = await second.fetchrow("SELECT * FROM payments")

        self.assertEqual(row["last_error"], "HTTP 503")
        self.assertEqual(row["attempt_count"], 1)
        self.assertIsNone(row["delivered_at"])
        self.assertIsNone(row["claim_token"])
        self.assertIsNone(row["lease_until"])
        self.assertIsNone(await claim_payment(second, 60))


        await second.execute(
            """
            UPDATE payments
            SET next_attempt_at =
                clock_timestamp() - INTERVAL '1 second'
            """
        )

        next_task = await claim_payment(second, 60)
        self.assertIsNotNone(next_task)
        self.assertEqual(next_task["attempt_count"], 2)


if __name__ == "__main__":
    unittest.main()