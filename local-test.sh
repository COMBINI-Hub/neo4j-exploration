#!/bin/bash

# Enable error handling
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Building Docker images...${NC}"

# Build the images
docker build -t kg-server ./server
docker build -t kg-client ./client

echo -e "${YELLOW}Creating Docker network...${NC}"

# Create a Docker network (or use existing one)
docker network create kg-network 2>/dev/null || true

echo -e "${YELLOW}Stopping and removing any existing containers...${NC}"

# Clean up any existing containers
docker rm -f kg-server kg-client 2>/dev/null || true

echo -e "${YELLOW}Starting server container...${NC}"

# Run the server
docker run -d --name kg-server \
    --network kg-network \
    -p 7474:7474 -p 7687:7687 -p 8080:8080 \
    kg-server

# Wait for services with timeout
echo -e "${YELLOW}Waiting for services to be ready...${NC}"
TIMEOUT=60
COUNTER=0
until curl -s http://localhost:7474 > /dev/null && curl -s http://localhost:8080/health > /dev/null; do
    if [ $COUNTER -gt $TIMEOUT ]; then
        echo -e "${RED}Services failed to start within $TIMEOUT seconds${NC}"
        echo -e "${YELLOW}Checking container logs:${NC}"
        docker logs kg-server
        exit 1
    fi
    echo "Waiting for services to start... ($COUNTER/$TIMEOUT seconds)"
    sleep 1
    COUNTER=$((COUNTER + 1))
done

echo -e "${GREEN}All services are ready!${NC}"

echo -e "${YELLOW}Starting client container...${NC}"

# Run the client
docker run --name kg-client \
    --network kg-network \
    -e NEO4J_URI=neo4j://kg-server:7687 \
    -e SERVER_URL=http://kg-server:8080 \
    -e NEO4J_USERNAME=neo4j \
    -e NEO4J_PASSWORD=yourpassword \
    kg-client

echo -e "${GREEN}Test completed!${NC}"