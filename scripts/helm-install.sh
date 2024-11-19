#!/usr/bin/env zsh

#!/usr/bin/env bash

# Build Docker images
docker build -t kg-server ./server
docker build -t kg-client ./client

# Install/upgrade server chart
helm upgrade --install kg-server ./charts/server \
    --namespace kg \
    --create-namespace \
    --wait

# Install/upgrade client chart
helm upgrade --install kg-client ./charts/client \
    --namespace kg \
    --create-namespace \
    --set client.config.neo4jUri="neo4j://kg-server-server:7687" \
    --wait

# Display connection information
echo "\nKnowledge Graph Server:"
echo "Neo4j Browser: http://localhost:7474"
echo "Bolt URI: neo4j://localhost:7687"
echo "\nKnowledge Graph Client:"
echo "Default credentials:"
echo "Username: neo4j"
echo "Password: yourpassword"
