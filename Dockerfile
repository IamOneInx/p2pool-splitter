FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir requests pyyaml

# Copy source
COPY splitter.py .

# Data directory (mount config.yaml and splitter.db here)
VOLUME ["/data"]

ENTRYPOINT ["python", "splitter.py", "--config", "/data/config.yaml"]
