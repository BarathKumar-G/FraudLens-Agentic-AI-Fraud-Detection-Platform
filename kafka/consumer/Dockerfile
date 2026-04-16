FROM python:3.11-slim

WORKDIR /app

COPY kafka/consumer/requirements.txt ./
RUN pip install --upgrade pip && \
    pip install --default-timeout=100 \
    --retries 5 \
    --no-cache-dir \
    -r requirements.txt

COPY . .
ENV PYTHONPATH=/app
CMD ["python", "kafka/consumer/realtime_inference.py"]