FROM python:3.12-slim
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
	pip install awslabs.aws-api-mcp-server==0.1.4
COPY . .
EXPOSE 9000
