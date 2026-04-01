#!/bin/bash

# Simple script to start Neo4j with PrimeKG

echo "Starting Neo4j container..."
docker-compose up -d

echo "Waiting for Neo4j to be ready..."
sleep 10

echo ""
echo "Neo4j is starting up!"
echo "Access Neo4j Browser at: http://localhost:7474"
echo "Username: neo4j"
echo "Password: neo4j"
echo ""
echo "To import PrimeKG data, run: ./import_data.sh"
echo "To view logs: docker-compose logs -f neo4j"

