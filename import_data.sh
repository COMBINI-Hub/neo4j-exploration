#!/bin/bash

# Check if Neo4j container is running
CONTAINER_RUNNING=false
if docker-compose ps | grep -q "neo4j.*Up"; then
  echo "Neo4j container is running."
  CONTAINER_RUNNING=true
  
  # Check if the required files exist in the import directory
  echo "Checking for required files..."
  if ! docker-compose exec neo4j test -f /import/concept.csv; then
    echo "Error: /import/concept.csv does not exist in the Neo4j container"
    exit 1
  fi

  if ! docker-compose exec neo4j test -f /import/concept_header.csv; then
    echo "Error: /import/concept_header.csv does not exist in the Neo4j container"
    exit 1
  fi

  if ! docker-compose exec neo4j test -f /import/predication.csv; then
    echo "Error: /import/predication.csv does not exist in the Neo4j container"
    exit 1
  fi

  if ! docker-compose exec neo4j test -f /import/predication_header.csv; then
    echo "Error: /import/predication_header.csv does not exist in the Neo4j container"
    exit 1
  fi
  
  if ! docker-compose exec neo4j test -f /import/connections.csv; then
    echo "Error: /import/connections.csv does not exist in the Neo4j container"
    exit 1
  fi

  if ! docker-compose exec neo4j test -f /import/connections_header.csv; then
    echo "Error: /import/connections_header.csv does not exist in the Neo4j container"
    exit 1
  fi
  
  # Stop the container after checking files
  echo "Stopping Neo4j container..."
  docker-compose stop neo4j
else
  echo "Neo4j container is not running."
  
  # Start the container temporarily to check files
  echo "Starting Neo4j container temporarily to check files..."
  docker-compose up -d neo4j
  sleep 5  # Give container time to start
  
  # Check if the required files exist
  echo "Checking for required files..."
  if ! docker-compose exec neo4j test -f /import/concept.csv; then
    echo "Error: /import/concept.csv does not exist in the Neo4j container"
    docker-compose stop neo4j
    exit 1
  fi

  if ! docker-compose exec neo4j test -f /import/predication.csv; then
    echo "Error: /import/predication.csv does not exist in the Neo4j container"
    docker-compose stop neo4j
    exit 1
  fi
  
  if ! docker-compose exec neo4j test -f /import/connections.csv; then
    echo "Error: /import/connections.csv does not exist in the Neo4j container"
    docker-compose stop neo4j
    exit 1
  fi
  
  # Stop the container after checking files
  echo "Stopping Neo4j container..."
  docker-compose stop neo4j
fi

# Remove the existing database (be careful with this!)
echo "Starting container to remove existing database..."
docker-compose up -d neo4j
sleep 3
echo "Removing existing database..."
docker-compose exec neo4j rm -rf /data/databases/neo4j
docker-compose stop neo4j

# Run the import command
echo "Importing data..."
docker-compose run --rm neo4j neo4j-admin database import full neo4j \
  --nodes=/import/concept_header.csv,/import/concept.csv \
  --nodes=/import/predication_header.csv,/import/predication.csv \
  --relationships=/import/connections_header.csv,/import/connections.csv \
  --delimiter="," \
  --overwrite-destination=true \
  --skip-bad-relationships
  --verbose
  neo4j

# Start Neo4j again
echo "Starting Neo4j with imported data..."
docker-compose up -d neo4j
echo "Import completed successfully!"