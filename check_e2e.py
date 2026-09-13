import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import httpx

from database import open_connection


API_URL = "http://127.0.0.1:8000"


async def post_event(client, path, payload, expected_inserted):
    response = await client.post(path, json=payload)

    if response.status_code != 202:
        raise AssertionError(
            f"{path}: ожидался 202, получен {response.status_code}"
        )

    result = response.json()
    if result.get("inserted") is not expected_inserted:
        raise AssertionError(
            f"{path}: неожиданный результат inserted: {result}"
        )


async def main():
    clid = "e2e-" + uuid4().hex
    payment_ts = datetime(2026, 9, 13, 12, 5, tzinfo=timezone.utc)

    click = {
        "clid": clid,
        "ad_id": 90,
        "click_spend": 1.25,
        "ts": "2026-09-13T12:00:00Z",
    }
    payment = {
        "clid": clid,
        "payout": 125.50,
        "ts": payment_ts.isoformat(),
    }

    connection = await open_connection()

    try:
        async with httpx.AsyncClient(
            base_url=API_URL,
            timeout=10,
            trust_env=False,
        ) as client:
            await post_event(
                client, "/events/payments", payment, True
            )

            # Даём worker несколько возможностей проверить очередь
            await asyncio.sleep(5)

            row = await connection.fetchrow(
                """
                SELECT delivered_at, attempt_count
                FROM payments
                WHERE clid = $1 AND ts = $2
                """,
                clid,
                payment_ts,
            )

            if row is None:
                raise AssertionError("Покупка не найдена в БД")
            if row["delivered_at"] is not None:
                raise AssertionError("Покупка доставлена без клика")
            if row["attempt_count"] != 0:
                raise AssertionError("Worker захватил покупку без клика")

            print("OK: покупка сохранена и ожидает клик")

            await post_event(client, "/events/clicks", click, True)

            async with asyncio.timeout(30):
                while True:
                    delivered_at = await connection.fetchval(
                        """
                        SELECT delivered_at
                        FROM payments
                        WHERE clid = $1 AND ts = $2
                        """,
                        clid,
                        payment_ts,
                    )
                    if delivered_at is not None:
                        break
                    await asyncio.sleep(0.5)

            print("OK: после прихода клика доставка подтверждена")

            await post_event(
                client, "/events/payments", payment, False
            )

            row = await connection.fetchrow(
                """
                SELECT count(*) AS total,
                       max(delivered_at) AS delivered_at
                FROM payments
                WHERE clid = $1 AND ts = $2
                """,
                clid,
                payment_ts,
            )

            if row["total"] != 1:
                raise AssertionError("Создан дубликат покупки")
            if row["delivered_at"] != delivered_at:
                raise AssertionError("Повтор изменил отметку доставки")

            print("OK: повтор не создал запись и не сбросил доставку")
            print("E2E пройден. clid:", clid)
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())