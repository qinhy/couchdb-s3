FROM python:3.13-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock README.md ./
COPY *.py ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir \
        fastapi \
        uvicorn \
        pydantic \
        pydantic-settings \
        requests \
        fsspec \
        s3fs

EXPOSE 8000

CMD ["uvicorn", "backup_api:app", "--host", "0.0.0.0", "--port", "8000"]
