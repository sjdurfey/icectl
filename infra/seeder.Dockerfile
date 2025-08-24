FROM python:3.12-slim

RUN pip install --no-cache-dir pyiceberg==0.9.1 pyarrow==21.0.0 requests==2.32.3 s3fs==2024.9.0

WORKDIR /app
COPY infra/seeder.py /app/seeder.py

CMD ["python", "/app/seeder.py"]

