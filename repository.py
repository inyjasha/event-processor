import asyncpg
from schemas import ClickEvent, PaymentEvent
import logging
from uuid import uuid4, UUID
from datetime import datetime

logger = logging.getLogger(__name__)

async def insert_click(
        connection: asyncpg.Connection,
        event: ClickEvent,
) -> bool:
    inserted_clid = await connection.fetchval(
        """
        INSERT INTO clicks (clid, ad_id, click_spend, ts)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (clid) DO NOTHING
        RETURNING clid;
        """,
        event.clid,
        event.ad_id,
        event.click_spend,
        event.ts,
        
    )

    if inserted_clid is None:
        existing_click = await connection.fetchrow(
            """
            SELECT ad_id, click_spend, ts
            FROM clicks
            WHERE clid = $1;
            """,
            event.clid,
        )

        if existing_click is not None:
            has_conflict = (
                existing_click["ad_id"] != event.ad_id
                or existing_click["click_spend"] != event.click_spend
                or existing_click["ts"] != event.ts
            )

            if has_conflict:
                logger.warning(
                    "Повторный клик содержит другие данные: clid=%s",
                    event.clid,
                )

    
    return inserted_clid is not None

async def insert_payment (
        connection: asyncpg.Connection,
        event: PaymentEvent,
) -> bool:
    inserted_clid = await connection.fetchval(
        """
        INSERT INTO payments (clid, payout, ts)
        VALUES ($1, $2, $3)
        ON CONFLICT (clid, ts) DO NOTHING
        RETURNING clid;
        """,
        event.clid,
        event.payout,
        event.ts,
    )

    if inserted_clid is None:
        existing_payment = await connection.fetchrow(
            """
            SELECT payout
            FROM payments
            WHERE clid = $1 AND ts = $2;
            """,
            event.clid,
            event.ts,
        )

        if existing_payment is not None:
            if existing_payment["payout"] != event.payout:
                logger.warning(
                    "Повторная покупка содержит другую сумму: clid=%s, ts=%s",
                    event.clid,
                    event.ts,
                )

    return inserted_clid is not None

async def claim_payment(
        connection: asyncpg.Connection,
        lease_seconds: int,
) -> dict | None:
    claim_token = uuid4()
    async with connection.transaction():
            payment = await connection.fetchrow(
            """
            SELECT
                p.clid,
                p.ts,
                p.payout,
                c.click_spend,
                c.ts AS click_ts
            FROM payments AS p
            INNER JOIN clicks AS c ON c.clid = p.clid
            WHERE p.delivered_at IS NULL
              AND p.next_attempt_at <= CURRENT_TIMESTAMP
              AND (
                  p.lease_until IS NULL
                  OR p.lease_until <= CURRENT_TIMESTAMP
              )
            ORDER BY p.next_attempt_at ASC, p.clid ASC, p.ts ASC
            LIMIT 1
            FOR UPDATE OF p SKIP LOCKED;
            """
        )

            if payment is None:
                return None
            
            claimed = await connection.fetchrow(
            """
            UPDATE payments
            SET
                lease_until = clock_timestamp()
                    + $3 * INTERVAL '1 second',
                claim_token = $4,
                attempt_count = attempt_count + 1
            WHERE clid = $1 AND ts = $2
            RETURNING claim_token, lease_until, attempt_count;
            """,
            payment["clid"],
            payment["ts"],
            lease_seconds,
            claim_token,
            )

            if claimed is None:
                raise RuntimeError(
                    "Не удалось записать захват выбранной покупки"
                )

            task = dict(payment)
            task.update(dict(claimed))

    return task

async def mark_delivered(
        connection: asyncpg.Connection,
        clid: str,
        payment_ts: datetime,
        claim_token: UUID,
) -> bool:
    updated_clid = await connection.fetchval(
        """
        UPDATE payments
        SET
            delivered_at = clock_timestamp(),
            last_error = NULL,
            lease_until = NULL,
            claim_token = NULL
        WHERE clid = $1
          AND ts = $2
          AND claim_token = $3
          AND delivered_at IS NULL
        RETURNING clid;
        """,
        clid,
        payment_ts,
        claim_token,
    )

    return updated_clid is not None


async def schedule_retry(
        connection: asyncpg.Connection,
        clid: str,
        payment_ts: datetime,
        claim_token: UUID,
        error_message: str,
        retry_delay_seconds: float,
) -> bool:
    updated_clid = await connection.fetchval(
        """
        UPDATE payments
        SET
            last_error = $4,
            next_attempt_at = clock_timestamp()
                + $5 * INTERVAL '1 second',
            lease_until = NULL,
            claim_token = NULL
        WHERE clid = $1
          AND ts = $2
          AND claim_token = $3
          AND delivered_at IS NULL
        RETURNING clid;
        """,
        clid,
        payment_ts,
        claim_token,
        error_message,
        retry_delay_seconds,
    )

    return updated_clid is not None