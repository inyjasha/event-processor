FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN useradd --create-home appuser

COPY --chown=appuser:appuser *.py ./
COPY --chown=appuser:appuser schema.sql ./
COPY --chown=appuser:appuser migrations/ ./migrations/

USER appuser

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]