FROM python:3.11-slim

WORKDIR /app

COPY kafka/producer/requirements.txt ./
RUN pip install --upgrade pip && \
    pip install --default-timeout=100 \
    --retries 5 \
    --no-cache-dir \
    -r requirements.txt

COPY kafka/producer/ ./kafka/producer/
ENV PYTHONPATH=/app
CMD ["python", "kafka/producer/producer.py"]