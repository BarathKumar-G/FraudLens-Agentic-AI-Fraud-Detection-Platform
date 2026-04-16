FROM python:3.11-slim

WORKDIR /app/backend

COPY backend/requirements.txt ./
RUN pip install --upgrade pip && \
    pip install --default-timeout=100 \
    --retries 5 \
    --no-cache-dir \
    -r requirements.txt

COPY backend/ .
EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]