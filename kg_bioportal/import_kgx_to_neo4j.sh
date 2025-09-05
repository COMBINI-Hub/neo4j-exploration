#!/bin/bash
set -e

# Configuration (relative to project root)
NODES_HEADER="kg_bioportal/import/nodes_header.csv"
EDGES_HEADER="kg_bioportal/import/edges_header.csv"
NODES_CSV="kg_bioportal/import/nodes_for_neo4j.csv"
EDGES_CSV="kg_bioportal/import/edges_for_neo4j.csv"
DB_NAME="bioportal"
DOCKER_COMPOSE_FILE="kg_bioportal/docker-compose.yml"
SERVICE_NAME="neo4j-bioportal"
IMAGE="neo4j:2025.03.0"
DATA_VOLUME="kg_bioportal_neo4j_bioportal_data"
LOGS_VOLUME="kg_bioportal_neo4j_bioportal_logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if CSV files exist and are not directories
log "Checking for Neo4j-admin CSV files..."
if [ ! -f "$NODES_HEADER" ]; then
    error "Nodes header file not found: $NODES_HEADER"
    exit 1
fi
if [ ! -f "$EDGES_HEADER" ]; then
    error "Edges header file not found: $EDGES_HEADER"
    exit 1
fi
if [ ! -f "$NODES_CSV" ]; then
    error "Nodes CSV file not found: $NODES_CSV"
    exit 1
fi
if [ ! -f "$EDGES_CSV" ]; then
    error "Edges CSV file not found: $EDGES_CSV"
    exit 1
fi
if [ -d "$NODES_CSV" ]; then
    error "Nodes CSV path is a directory, not a file: $NODES_CSV"
    exit 1
fi
if [ -d "$EDGES_CSV" ]; then
    error "Edges CSV path is a directory, not a file: $EDGES_CSV"
    exit 1
fi
success "Found Neo4j-admin CSV and header files."

log "File sizes: Nodes=$(du -h "$NODES_CSV" | cut -f1), Edges=$(du -h "$EDGES_CSV" | cut -f1)"

log "Checking for any processes using port 7474 (Neo4j default HTTP)..."
PIDS=$(lsof -ti tcp:7474 || true)
if [ -n "$PIDS" ]; then
    warning "Found processes on port 7474: $PIDS. Killing them..."
    kill -9 $PIDS || true
    success "Killed processes on port 7474."
else
    success "No processes found on port 7474."
fi

# Stop and remove the running Neo4j Docker container (if any)
log "Stopping and removing $SERVICE_NAME Docker service (if running)..."
docker-compose -f "$DOCKER_COMPOSE_FILE" stop "$SERVICE_NAME" || true
docker-compose -f "$DOCKER_COMPOSE_FILE" rm -f "$SERVICE_NAME" || true
success "$SERVICE_NAME stopped and removed (if it was running)."

# Remove Docker volumes for a clean start
log "Removing Docker volumes $DATA_VOLUME and $LOGS_VOLUME for a clean import..."
docker volume rm $DATA_VOLUME || true
docker volume rm $LOGS_VOLUME || true
success "Docker volumes removed."

# Run neo4j-admin import in a temporary container with fresh volumes
log "Running neo4j-admin import inside a temporary Docker container (fresh volumes)..."
docker run --rm \
  -v "$(pwd)/kg_bioportal/import":/var/lib/neo4j/import \
  -v $DATA_VOLUME:/data \
  -v $LOGS_VOLUME:/logs \
  "$IMAGE" \
  neo4j-admin database import full --verbose \
    --nodes="/var/lib/neo4j/import/nodes_header.csv,/var/lib/neo4j/import/nodes_for_neo4j.csv" \
    --relationships="/var/lib/neo4j/import/edges_header.csv,/var/lib/neo4j/import/edges_for_neo4j.csv" \
    --overwrite-destination=true \
    --skip-bad-relationships \
    --bad-tolerance=1000000 \
    "$DB_NAME"

IMPORT_EXIT_CODE=$?
if [ $IMPORT_EXIT_CODE -ne 0 ]; then
    error "neo4j-admin import failed. Check the output above for details."
    exit $IMPORT_EXIT_CODE
fi
success "neo4j-admin import completed successfully!"

# Verify that data was actually written to the database files
log "Verifying data was written to database files..."
docker run --rm \
  -v $DATA_VOLUME:/data \
  alpine:latest \
  sh -c "ls -la /data/databases/$DB_NAME/ && echo '--- Node store size ---' && ls -lh /data/databases/$DB_NAME/neostore.nodestore.db && echo '--- Relationship store size ---' && ls -lh /data/databases/$DB_NAME/neostore.relationshipstore.db"

# Check if the database files have actual content (not 0 bytes)
NODE_SIZE=$(docker run --rm -v $DATA_VOLUME:/data alpine:latest sh -c "stat -c%s /data/databases/$DB_NAME/neostore.nodestore.db 2>/dev/null || echo '0'")
REL_SIZE=$(docker run --rm -v $DATA_VOLUME:/data alpine:latest sh -c "stat -c%s /data/databases/$DB_NAME/neostore.relationshipstore.db 2>/dev/null || echo '0'")

if [ "$NODE_SIZE" -eq 0 ] || [ "$REL_SIZE" -eq 0 ]; then
    error "Database files are empty (0 bytes). Import may have failed silently."
    error "Node store size: $NODE_SIZE bytes"
    error "Relationship store size: $REL_SIZE bytes"
    exit 1
fi

success "Database files verified - Node store: $NODE_SIZE bytes, Relationship store: $REL_SIZE bytes"

# Start Neo4j Docker container again
log "Starting $SERVICE_NAME Docker service with fresh data..."
docker-compose -f "$DOCKER_COMPOSE_FILE" up -d "$SERVICE_NAME"
success "$SERVICE_NAME started."

log "If you want to use the new database, ensure initial.dbms.default_database=$DB_NAME is set in your neo4j.conf (already set in docker-compose)."
success "Neo4j-admin import and restart complete!" 