#!/bin/bash
set -e

echo "Starting Neo4j..."
neo4j start

echo "Waiting for Neo4j to be ready..."
until curl -s http://localhost:7474 > /dev/null; do
    echo "Neo4j not ready yet..."
    sleep 2
done

echo "Neo4j is ready!"

# Extract username and password from NEO4J_AUTH
IFS='/' read -r username password <<< "${NEO4J_AUTH}"

# Export Neo4j connection details for FastAPI
export NEO4J_URI="neo4j://localhost:7687"
export NEO4J_USERNAME="${username}"
export NEO4J_PASSWORD="${password}"

echo "Starting FastAPI server..."
python3 server.py &
SERVER_PID=$!

# Wait for FastAPI to start (with timeout)
TIMEOUT=30
COUNTER=0
echo "Waiting for FastAPI server to be ready..."
until curl -s http://localhost:8080/health > /dev/null 2>&1; do
    if [ $COUNTER -gt $TIMEOUT ]; then
        echo "FastAPI server failed to start within $TIMEOUT seconds"
        exit 1
    fi
    echo "FastAPI not ready yet..."
    sleep 1
    COUNTER=$((COUNTER + 1))
done

echo "FastAPI server is ready!"

# Keep the container running by waiting for the FastAPI process
wait $SERVER_PID