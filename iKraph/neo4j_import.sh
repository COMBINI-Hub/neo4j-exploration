#!/bin/bash
# Neo4j Import Script for Compressed Files (Docker Compose Best Practice)
# Follows robust pattern: checks, stops, imports, restarts

set -e  # Exit on any error

# Configuration
CONTAINER_NAME="neo4j-ikraph"
DB_NAME="ikraph"
IMPORT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/import" && pwd)"
REQUIRED_FILES=(
  nodes_pathway.csv.gz
  nodes_pharmacologic\ class.csv.gz
  nodes_species.csv.gz
  nodes_dnamutation.csv.gz
  nodes_anatomy.csv.gz
  nodes_gene.csv.gz
  nodes_chemical.csv.gz
  nodes_cellular\ component.csv.gz
  nodes_molecular\ function.csv.gz
  nodes_biological\ process.csv.gz
  nodes_cellline.csv.gz
  nodes_disease.csv.gz
  relationships_db.csv.gz
  relationships_pubmed.csv.gz
)

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

# Check for required files in import directory
log "Checking for required files in $IMPORT_DIR..."
for f in "${REQUIRED_FILES[@]}"; do
  if [ ! -f "$IMPORT_DIR/$f" ]; then
    error "Required file missing: $IMPORT_DIR/$f"
    exit 1
  fi
done
success "All required files found."

# Stop the container if running
if docker ps | grep -q "$CONTAINER_NAME"; then
  log "Stopping running Neo4j container..."
  docker-compose stop "$CONTAINER_NAME"
else
  log "Neo4j container is not running."
fi

# Remove the existing database (be careful with this!)
log "Removing existing database..."
docker-compose run --rm "$CONTAINER_NAME" rm -rf /data/databases/$DB_NAME /data/transactions/$DB_NAME || true

# Run the import command (neo4j-admin import requires the database to be stopped)
log "Importing data using neo4j-admin..."
docker-compose run --rm "$CONTAINER_NAME" neo4j-admin database import full "$DB_NAME" \
  --nodes="pathway=/var/lib/neo4j/import/nodes_pathway.csv.gz" \
  --nodes="pharmacologic_class=/var/lib/neo4j/import/nodes_pharmacologic class.csv.gz" \
  --nodes="species=/var/lib/neo4j/import/nodes_species.csv.gz" \
  --nodes="dnamutation=/var/lib/neo4j/import/nodes_dnamutation.csv.gz" \
  --nodes="anatomy=/var/lib/neo4j/import/nodes_anatomy.csv.gz" \
  --nodes="gene=/var/lib/neo4j/import/nodes_gene.csv.gz" \
  --nodes="chemical=/var/lib/neo4j/import/nodes_chemical.csv.gz" \
  --nodes="cellular_component=/var/lib/neo4j/import/nodes_cellular component.csv.gz" \
  --nodes="molecular_function=/var/lib/neo4j/import/nodes_molecular function.csv.gz" \
  --nodes="biological_process=/var/lib/neo4j/import/nodes_biological process.csv.gz" \
  --nodes="cellline=/var/lib/neo4j/import/nodes_cellline.csv.gz" \
  --nodes="disease=/var/lib/neo4j/import/nodes_disease.csv.gz" \
  --relationships="db_relationship=/var/lib/neo4j/import/relationships_db.csv.gz" \
  --relationships="pubmed_relationship=/var/lib/neo4j/import/relationships_pubmed.csv.gz" \
  --overwrite-destination=true \
  --skip-bad-relationships=true \
  --skip-duplicate-nodes=true \
  --bad-tolerance=1000000 \
  --verbose

if [ $? -eq 0 ]; then
    success "Neo4j import completed successfully."
else
    error "Neo4j import failed."
    exit 1
fi

# Start Neo4j again
log "Starting Neo4j container with imported data..."
docker-compose up -d "$CONTAINER_NAME"
success "Import complete! Neo4j is starting up."
log "You can access Neo4j Browser at: http://localhost:7474"
log "Database name: $DB_NAME"
