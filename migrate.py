import asyncio
import hashlib
import logging
import re
from pathlib import Path

from database import open_connection


logger = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"

MIGRATION_LOCK_ID = 719042815


async def main():
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not files:
        raise RuntimeError("Не найдены SQL-миграции")

    versions = set()
    migrations = []

    for path in files:
        if not re.fullmatch(r"\d{3}_[a-z0-9_]+\.sql", path.name):
            raise RuntimeError(f"Некорректное имя миграции: {path.name}")

        version = path.name.split("_", 1)[0]
        if version in versions:
            raise RuntimeError(f"Повтор номера миграции: {version}")
        versions.add(version)

    
        sql = path.read_text(encoding="utf-8-sig")
        checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
        migrations.append((path.name, sql, checksum))

    connection = await open_connection()

    try:
        await connection.execute("SET search_path TO public")
        await connection.execute("SET lock_timeout = '30s'")
        await connection.execute("SET statement_timeout = '5min'")

        logger.info("Ожидание блокировки миграций")
        await connection.execute(
            "SELECT pg_advisory_lock($1::bigint)",
            MIGRATION_LOCK_ID,
        )

        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS public.schema_migrations (
                name TEXT PRIMARY KEY,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        rows = await connection.fetch(
            "SELECT name, checksum FROM public.schema_migrations"
        )
        applied = {row["name"]: row["checksum"] for row in rows}
        available = {name for name, _, _ in migrations}

        missing = set(applied) - available
        if missing:
            raise RuntimeError(
                "Отсутствуют ранее применённые миграции: "
                + ", ".join(sorted(missing))
            )

        
        for name, _, checksum in migrations:
            if name in applied and applied[name] != checksum:
                raise RuntimeError(
                    f"Применённая миграция изменена: {name}. "
                    "Создайте новую миграцию вместо редактирования старой."
                )

            if name not in applied and applied and name < max(applied):
                raise RuntimeError(
                    f"Новая миграция нарушает порядок истории: {name}"
                )

        applied_count = 0

        for name, sql, checksum in migrations:
            if name in applied:
                logger.info("Уже применена: %s", name)
                continue

            logger.info("Применяется: %s", name)

            async with connection.transaction():
                await connection.execute(sql)
                await connection.execute(
                    """
                    INSERT INTO public.schema_migrations (name, checksum)
                    VALUES ($1, $2)
                    """,
                    name,
                    checksum,
                )

            applied_count += 1
            logger.info("Применена: %s", name)

        logger.info("Миграции завершены. Новых: %s", applied_count)

    finally:
        await connection.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )
    asyncio.run(main())