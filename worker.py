import asyncio
import logging
from database import create_pool
import os
import math
import random
from advantage_client import create_http_client, send_payment
from repository import claim_payment, mark_delivered, schedule_retry
import httpx

logger = logging.getLogger(__name__)

POLL_INTERVAL = float(os.environ["WORKER_POLL_INTERVAL"])
if not math.isfinite(POLL_INTERVAL) or POLL_INTERVAL <=0:
    raise ValueError("WORKER_POLL_INTERVAL должен быть больше 0")

DB_OPERATION_TIMEOUT = 10
SEND_TOTAL_TIMEOUT = 20

RETRY_BASE_SECONDS = 2
RETRY_MAX_SECONDS = 300

LEASE_SECONDS = int(os.environ["WORKER_LEASE_SECONDS"])
if LEASE_SECONDS <= 2 * DB_OPERATION_TIMEOUT + SEND_TOTAL_TIMEOUT:
    raise ValueError("WORKER_LEASE_SECONDS должен быть больше 40 секунд")

def retry_delay(attempt_count: int) -> float:
    exponent = min(max(attempt_count -1, 0), 20)
    ceiling = min (
        RETRY_MAX_SECONDS,
        RETRY_BASE_SECONDS * (2 ** exponent),
    )
    return random.uniform(ceiling/2, ceiling)


async def get_task(pool) -> dict | None:
    async with asyncio.timeout(DB_OPERATION_TIMEOUT):
        async with pool.acquire(timeout=5) as connection:
            return await claim_payment(
                connection,
                lease_seconds=LEASE_SECONDS,
            )


async def save_result(
        pool,
        task: dict,
        error_message: str | None,
) -> None:
    delay = None

    async with asyncio.timeout(DB_OPERATION_TIMEOUT):
        async with pool.acquire(timeout=5) as connection:
            if error_message is None:
                updated = await mark_delivered(
                    connection,
                    clid=task["clid"],
                    payment_ts=task["ts"],
                    claim_token=task["claim_token"],
                )
            else:
                delay = retry_delay(task["attempt_count"])
                updated = await schedule_retry(
                    connection,
                    clid=task["clid"],
                    payment_ts=task["ts"],
                    claim_token=task["claim_token"],
                    error_message=error_message,
                    retry_delay_seconds=delay,
                )
        if not updated:
            logger.warning(
                "Результат не записан: захват изменился или задача завершена. "
            "clid=%s, payment_ts=%s",
            task["clid"],
            task["ts"],
            )
        elif error_message is None:
            logger.info(
                "Доставлено: clid=%s, payment_ts=%s, attempt=%s",
            task["clid"],
            task["ts"],
            task["attempt_count"],
            )
        else:
            logger.warning(
                "Повтор через %.1f с: clid=%s, payment_ts=%s, "
            "attempt=%s, reason=%s",
            delay,
            task["clid"],
            task["ts"],
            task["attempt_count"],
            error_message,
            )

async def process_task(pool, client,task:dict) -> None:
    error_message = None

    try:
        async with asyncio.timeout(SEND_TOTAL_TIMEOUT):
            status = await send_payment(client, task)

            if status != 200:
                error_message = f"HTTP {status}"

    except (TimeoutError, httpx.TimeoutException):
        error_message =  "Истекло время ожидания HTTP-запроса"

    except httpx.RequestError as exc:
        error_message = f"Сетевая ошибка: {type(exc).__name__}"

    except Exception as exc:
        error_message = f"Ошибка отправки: {type(exc).__name__}"
        logger.error(
            "Ошибка подготовки или отправки: clid=%s, type=%s",
            task["clid"],
            type(exc).__name__,
        )
    await save_result(pool, task, error_message)

async def open_pool_with_retry():
    while True:
        try:
            async with asyncio.timeout(DB_OPERATION_TIMEOUT):
                return await create_pool()
        except (OSError, TimeoutError) as exc:
            logger.warning(
                "БД пока недоступна: %s. Повтор подключения через %.1f с",
                type(exc).__name__,
                POLL_INTERVAL,
            )
            await asyncio.sleep(POLL_INTERVAL)



async def main():
    pool = await open_pool_with_retry()

    try: 
        async with create_http_client() as client:
            logger.info("Worker запущен")

            while True:
                try:
                    task = await get_task(pool)

                    if task is None:
                        await asyncio.sleep(POLL_INTERVAL)
                        continue

                    logger.info(
                        "Захвачена задача: clid=%s, payment_ts=%s, attempt=%s",
                        task["clid"],
                        task["ts"],
                        task["attempt_count"]
                    )

                    await process_task(pool, client, task)
                except asyncio.CancelledError:
                    raise

                except Exception:
                    logger.exception(
                        "Ошибка обработки очереди. "
                        "Незавершённые задачи остаются в БД"
                    )
                    await asyncio.sleep(POLL_INTERVAL)

    finally:
        try:
            async with asyncio.timeout(DB_OPERATION_TIMEOUT):
                await pool.close()
        except TimeoutError:
            pool.terminate()
            logger.warning("Пул закрыт принудительно после тайм-аута")

        logger.info("Пул соединений worker закрыт")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker остановлен пользователем")





    