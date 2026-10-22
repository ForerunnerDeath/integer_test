FROM python:3.12-slim AS base
WORKDIR /service
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/service
COPY . .
RUN pip install --no-cache-dir .
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000","--workers","4"]

FROM base AS test
RUN pip install --no-cache-dir ".[dev]"
CMD ["pytest","-q"]
