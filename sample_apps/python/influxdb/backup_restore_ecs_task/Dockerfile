FROM python:3.13-slim@sha256:739e7213785e88c0f702dcdc12c0973afcbd606dbf021a589cab77d6b00b579d

RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install required packages
RUN apt-get update && \
    apt-get install -y wget tar curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Influx CLI
RUN wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-arm64.tar.gz && \
    echo "867c3cbabd63a34a9b1ac643fd5c5d268b694acc98e3b75fa5a78d63037097dd influxdb2-client-2.7.5-linux-arm64.tar.gz" | sha256sum --check && \
    tar -xzf influxdb2-client-2.7.5-linux-arm64.tar.gz && \
    mv influx /usr/local/bin/ && \
    chmod +x /usr/local/bin/influx && \
    rm influxdb2-client-2.7.5-linux-arm64.tar.gz

# Create data directory for backups/restores
RUN mkdir -p /data && chmod 777 /data

WORKDIR /app

COPY backup_restore.py /app/
COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

USER appuser

# Set the entrypoint
ENTRYPOINT ["python", "/app/backup_restore.py"]
