import unittest
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
import simplejson

import worker
from advantage_client import (
    ADVANTAGE_TOKEN,
    CLICK_SPEND_CURRENCY,
    PAYOUT_CURRENCY,
    create_http_client,
    send_payment,
)


def make_task():
    return {
        "clid": "automated-test",
        "ts": datetime(2026, 9, 13, 10, 5, tzinfo=timezone.utc),
        "payout": Decimal("123456789.123456789"),
        "click_spend": Decimal("0.123456789"),
        "click_ts": datetime(2026, 9, 13, 10, 0, tzinfo=timezone.utc),
        "claim_token": uuid4(),
        "attempt_count": 1,
    }


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_payload_preserves_decimal_and_contract(self):
        task = make_task()

        async def handler(request):
            self.assertEqual(request.method, "POST")
            self.assertEqual(
                request.headers["content-type"],
                "application/json",
            )

            payload = simplejson.loads(
                request.content.decode("utf-8"),
                use_decimal=True,
            )

            self.assertEqual(
                set(payload),
                {
                    "clid",
                    "payout",
                    "click_spend",
                    "click_ts",
                    "payment_ts",
                    "payout_currency",
                    "click_spend_currency",
                },
            )
            self.assertEqual(payload["clid"], task["clid"])
            self.assertEqual(payload["payout"], task["payout"])
            self.assertEqual(payload["click_spend"], task["click_spend"])
            self.assertEqual(
                payload["click_ts"], task["click_ts"].isoformat()
            )
            self.assertEqual(
                payload["payment_ts"], task["ts"].isoformat()
            )
            self.assertEqual(payload["payout_currency"], PAYOUT_CURRENCY)
            self.assertEqual(
                payload["click_spend_currency"], CLICK_SPEND_CURRENCY
            )
            return httpx.Response(200)

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ) as client:
            self.assertEqual(await send_payment(client, task), 200)

    async def test_client_has_auth_and_redirects_disabled(self):
        async with create_http_client() as client:
            self.assertTrue(
                client.headers["Authorization"] == f"Bearer {ADVANTAGE_TOKEN}",
                "Некорректный заголовок авторизации",
            )
            self.assertFalse(client.follow_redirects)

    async def test_non_200_status_is_returned_without_redirect(self):
        for status in (201, 202, 204, 302, 400, 401, 429, 500, 503):
            with self.subTest(status=status):
                requests = []

                async def handler(request):
                    requests.append(request)
                    return httpx.Response(
                        status,
                        headers={"Location": "https://example.invalid/next"},
                    )

                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler),
                    follow_redirects=False,
                ) as client:
                    result = await send_payment(client, make_task())

                self.assertEqual(result, status)
                self.assertEqual(len(requests), 1)


class WorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_200_is_delivery_success(self):
        pool = object()
        client = object()
        task = make_task()

        for status in (200, 201, 202, 204, 302, 400, 401, 429, 500, 503):
            with self.subTest(status=status):
                with (
                    patch.object(
                        worker,
                        "send_payment",
                        new_callable=AsyncMock,
                        return_value=status,
                    ),
                    patch.object(
                        worker,
                        "save_result",
                        new_callable=AsyncMock,
                    ) as save,
                ):
                    await worker.process_task(pool, client, task)

                    expected_error = None if status == 200 else f"HTTP {status}"
                    save.assert_awaited_once_with(pool, task, expected_error)

    async def test_network_errors_schedule_retry(self):
        for exception in (
            httpx.ConnectError("connection unavailable"),
            httpx.ReadTimeout("read timeout"),
            TimeoutError(),
        ):
            with self.subTest(exception=type(exception).__name__):
                task = make_task()
                pool = object()

                with (
                    patch.object(
                        worker,
                        "send_payment",
                        new_callable=AsyncMock,
                        side_effect=exception,
                    ),
                    patch.object(
                        worker,
                        "save_result",
                        new_callable=AsyncMock,
                    ) as save,
                ):
                    await worker.process_task(pool, object(), task)

                    save.assert_awaited_once()
                    self.assertIs(save.await_args.args[0], pool)
                    self.assertIs(save.await_args.args[1], task)
                    self.assertIsInstance(save.await_args.args[2], str)

    async def test_total_send_timeout(self):
        import asyncio

        async def slow_send(*args):
            await asyncio.sleep(10)
            return 200

        with (
            patch.object(worker, "SEND_TOTAL_TIMEOUT", 0.01),
            patch.object(worker, "send_payment", side_effect=slow_send),
            patch.object(
                worker,
                "save_result",
                new_callable=AsyncMock,
            ) as save,
        ):
            await worker.process_task(object(), object(), make_task())

            save.assert_awaited_once()
            self.assertIsNotNone(save.await_args.args[2])

    async def test_db_failure_after_send_is_not_hidden(self):
        with (
            patch.object(
                worker,
                "send_payment",
                new_callable=AsyncMock,
                return_value=200,
            ),
            patch.object(
                worker,
                "save_result",
                new_callable=AsyncMock,
                side_effect=RuntimeError("database unavailable"),
            ),
        ):
            with self.assertRaises(RuntimeError):
                await worker.process_task(object(), object(), make_task())


class RetryTests(unittest.TestCase):
    def test_retry_delay_is_positive_and_bounded(self):
        for attempt in (1, 2, 5, 10, 100, 1000000):
            ceiling = min(
                worker.RETRY_MAX_SECONDS,
                worker.RETRY_BASE_SECONDS
                * (2 ** min(max(attempt - 1, 0), 20)),
            )

            for _ in range(20):
                delay = worker.retry_delay(attempt)
                self.assertGreater(delay, 0)
                self.assertGreaterEqual(delay, ceiling / 2)
                self.assertLessEqual(delay, ceiling)


if __name__ == "__main__":
    unittest.main()