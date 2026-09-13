import argparse
import asyncio
import json
import math
import platform
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import httpx

from database import open_connection


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * fraction) - 1)
    return round(ordered[index], 2)


async def main(args):
    if args.pairs < 1 or args.concurrency < 1:
        raise ValueError("pairs и concurrency должны быть положительными")
    if args.drain_timeout <= 0:
        raise ValueError("drain-timeout должен быть положительным")

    run_id = "load-" + uuid4().hex
    prefix = run_id + "-"

    click_ts = "2026-09-13T12:00:00Z"
    payment_ts = "2026-09-13T12:05:00Z"

    jobs = asyncio.Queue()

    for index in range(args.pairs):
        clid = f"{prefix}{index}"

        click = (
            "/events/clicks",
            {
                "clid": clid,
                "ad_id": 100,
                "click_spend": 1.25,
                "ts": click_ts,
            },
        )
        payment = (
            "/events/payments",
            {
                "clid": clid,
                "payout": 25.00,
                "ts": payment_ts,
            },
        )

        pair = (payment, click) if index % 2 == 0 else (click, payment)
        for job in pair:
            jobs.put_nowait(job)

    latencies_ms = []
    accepted_latencies_ms = []
    outcomes = Counter()
    connection = await open_connection()

    async def snapshot():
        return await connection.fetchrow(
            """
            SELECT
                count(*) AS payments,
                count(*) FILTER (
                    WHERE delivered_at IS NOT NULL
                ) AS delivered,
                count(*) FILTER (
                    WHERE delivered_at IS NULL
                ) AS pending,
                coalesce(sum(attempt_count), 0) AS attempts
            FROM payments
            WHERE starts_with(clid, $1)
            """,
            prefix,
        )

    limits = httpx.Limits(
        max_connections=args.concurrency,
        max_keepalive_connections=args.concurrency,
    )

    try:
        async with httpx.AsyncClient(
            base_url=args.url,
            timeout=10,
            limits=limits,
            trust_env=False,
        ) as client:

            async def sender():
                while True:
                    try:
                        path, payload = jobs.get_nowait()
                    except asyncio.QueueEmpty:
                        return

                    started = time.perf_counter()
                    accepted = False

                    try:
                        response = await client.post(path, json=payload)

                        if response.status_code == 202:
                            body = response.json()
                            if body.get("inserted") is True:
                                outcomes["accepted_new"] += 1
                                accepted = True
                            else:
                                outcomes["unexpected_202_body"] += 1
                        else:
                            outcomes[f"http_{response.status_code}"] += 1

                    except httpx.RequestError as exc:
                        outcomes[type(exc).__name__] += 1
                    except ValueError:
                        outcomes["invalid_json_response"] += 1
                    finally:
                        elapsed = (time.perf_counter() - started) * 1000
                        latencies_ms.append(elapsed)
                        if accepted:
                            accepted_latencies_ms.append(elapsed)
                        jobs.task_done()

            started_at = datetime.now(timezone.utc).isoformat()
            started = time.perf_counter()

            await asyncio.gather(
                *(sender() for _ in range(args.concurrency))
            )

            ingest_seconds = time.perf_counter() - started

        after_ingest = dict(await snapshot())
        print("Приём завершён:", dict(outcomes), flush=True)
        print("Очередь после приёма:", after_ingest, flush=True)

        drain_started = time.perf_counter()
        deadline = drain_started + args.drain_timeout
        final_state = after_ingest

        while (
            final_state["payments"] != args.pairs
            or final_state["delivered"] != args.pairs
        ):
            if time.perf_counter() >= deadline:
                break

            await asyncio.sleep(1)
            final_state = dict(await snapshot())

        drain_seconds = time.perf_counter() - drain_started
        total_seconds = time.perf_counter() - started

        complete = (
            final_state["payments"] == args.pairs
            and final_state["delivered"] == args.pairs
        )

        clicks_count = await connection.fetchval(
            """
            SELECT count(*)
            FROM clicks
            WHERE starts_with(clid, $1)
            """,
            prefix,
        )

        request_count = args.pairs * 2
        accepted_count = outcomes["accepted_new"]

        report = {
            "run_id": run_id,
            "started_at_utc": started_at,
            "generator_python": platform.python_version(),
            "generator_platform": platform.platform(),
            "api_url": args.url,
            "pairs": args.pairs,
            "requests": request_count,
            "concurrency": args.concurrency,
            "request_timeout_seconds": 10,
            "drain_timeout_seconds": args.drain_timeout,
            "ingest_seconds": round(ingest_seconds, 3),
            "completed_requests_per_second": round(
                request_count / ingest_seconds, 2
            ),
            "accepted_events_per_second": round(
                accepted_count / ingest_seconds, 2
            ),
            "unsuccessful_requests": request_count - accepted_count,
            "outcomes": dict(outcomes),
            "all_requests_p50_ms": percentile(latencies_ms, 0.50),
            "all_requests_p95_ms": percentile(latencies_ms, 0.95),
            "all_requests_p99_ms": percentile(latencies_ms, 0.99),
            "accepted_requests_p95_ms": percentile(
                accepted_latencies_ms, 0.95
            ),
            "database_clicks": clicks_count,
            "queue_after_ingest": after_ingest,
            "final_state": final_state,
            "drain_wait_seconds": round(drain_seconds, 3),
            "total_seconds": round(total_seconds, 3),
            "all_payments_delivered": complete,
            "average_deliveries_per_second_over_run": round(
                final_state["delivered"] / total_seconds, 2
            ),
        }

        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        print(json.dumps(report, ensure_ascii=False, indent=2))
        print("Отчёт сохранён:", output.resolve())

        if request_count != accepted_count or not complete:
            raise SystemExit(
                "Замер завершён с ошибками приёма или неполной доставкой. "
                "Подробности сохранены в отчёте."
            )

    finally:
        await connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pairs", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--drain-timeout", type=float, default=300)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--output", default="load-results/baseline.json")
    asyncio.run(main(parser.parse_args()))