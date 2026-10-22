FROM python:3.12-slim
WORKDIR /service
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/service
COPY . .
RUN pip install --no-cache-dir .
CMD ["uvicorn","app.main:app","--host","0.0.0.0","--port","8000","--workers","4"]
