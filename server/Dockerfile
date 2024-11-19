# Build stage
FROM python:3.9-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Final stage
FROM neo4j:5.13.0

# Install Python and required packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder
COPY --from=builder /root/.local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages

# Set working directory
WORKDIR /app

# Copy server application code and startup script
COPY src/ .

# Make startup script executable
RUN chmod +x /app/start.sh

# Neo4j Configuration
ENV NEO4J_AUTH=neo4j/yourpassword
ENV NEO4J_ACCEPT_LICENSE_AGREEMENT=yes
ENV NEO4J_server_default__advertised__address=localhost
ENV PYTHONPATH=/app:/usr/local/lib/python3.9/site-packages

# Start command
CMD ["/app/start.sh"]