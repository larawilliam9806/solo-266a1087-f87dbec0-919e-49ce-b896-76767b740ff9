FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV WQ_DATA_DIR=/app/data \
    WQ_DOWNSTREAM_DIR=/app/downstream

EXPOSE 5000

# 单实例 waitress（并发正确性依赖 SQLite BEGIN IMMEDIATE 与同目录 os.replace，
# 多容器/多 worker 部署时必须共享同一个 WQ_DATA_DIR 与 WQ_DOWNSTREAM_DIR）
CMD ["waitress-serve", "--host=0.0.0.0", "--port=5000", "--call", "wsgi:create_app"]
