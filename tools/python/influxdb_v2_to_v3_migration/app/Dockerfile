FROM python:3.14.1-slim

RUN groupadd -r appuser && useradd -r -g appuser appuser

# Install required packages.
RUN apt-get update && \
    apt-get install -y wget tar curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install the InfluxDB v2 CLI.
RUN wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-arm64.tar.gz && \
    tar -xzf influxdb2-client-2.7.5-linux-arm64.tar.gz && \
    mv influx /usr/local/bin/ && \
    chmod +x /usr/local/bin/influx && \
    rm influxdb2-client-2.7.5-linux-arm64.tar.gz

# Download and install the InfluxDB v2 daemon.
RUN curl --location -O \
    https://download.influxdata.com/influxdb/releases/v2.7.12/influxdb2-2.7.12_linux_arm64.tar.gz && \
    tar xvzf influxdb2-2.7.12_linux_arm64.tar.gz && \
    mv influxdb2-2.7.12/usr/bin/influxd /usr/local/bin/ && \
    chmod +x /usr/local/bin/influxd && \
    rm influxdb2-2.7.12_linux_arm64.tar.gz

# Create directory for backup data.
RUN mkdir -p /engine/data && chmod 777 /engine/data

WORKDIR /app

COPY influxdb_v2_to_v3_migration.py /app/
COPY influxdb_v3_ingestion.py /app/
COPY pyproject.toml /app/

RUN pip install .

USER appuser

ENTRYPOINT ["python", "/app/influxdb_v2_to_v3_migration.py"]
