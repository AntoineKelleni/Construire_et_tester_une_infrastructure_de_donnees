FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src/ /app/src/
RUN sed -i 's/\r$//' /app/src/run_etl.sh && chmod +x /app/src/run_etl.sh

RUN mkdir -p /app/data
ENTRYPOINT ["/bin/bash", "/app/src/run_etl.sh"]
