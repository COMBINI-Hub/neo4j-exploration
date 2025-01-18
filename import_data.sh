#!/bin/bash

# Enable logging of all commands
set -x

# Function for logging
log() {
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] $1"
}

# Base directory paths
IMPORT_DIR="/var/lib/neo4j/import"
DEMO_DATA_DIR="demo_data"
DATA_DIR="data"

log "Starting import process..."
log "Using import directory: $IMPORT_DIR"
log "Using demo data directory: $DEMO_DATA_DIR"
log "Using data directory: $DATA_DIR"

# Input file paths
PREDICATION_FILE="${DEMO_DATA_DIR}/predication.csv"
PREDICATION_AUX_FILE="${DEMO_DATA_DIR}/predication_aux.csv"
GENERIC_CONCEPT_FILE="${DEMO_DATA_DIR}/generic_concept.csv"
ENTITY_FILE="${DATA_DIR}/entity.gz"
CITATIONS_FILE="${DEMO_DATA_DIR}/citations.csv"
SENTENCES_FILE="${DEMO_DATA_DIR}/sentence.csv"

# Header file paths
CITATIONS_HEADER="citations_header.csv"
SENTENCES_HEADER="sentences_header.csv"
ENTITIES_HEADER="entities_header.csv"
PREDICATIONS_HEADER="predications_header.csv"

# Output file paths
MERGED_PREDICATIONS="${IMPORT_DIR}/merged_predications.csv"
ENRICHED_ENTITIES="${IMPORT_DIR}/enriched_entities.csv"
IMPORT_CITATIONS="${IMPORT_DIR}/citations.csv"
IMPORT_SENTENCES="${IMPORT_DIR}/sentence.csv"

# Set working directory to Neo4j import directory
cd $IMPORT_DIR
log "Changed working directory to: $IMPORT_DIR"

# Create import directory if it doesn't exist
mkdir -p $IMPORT_DIR
log "Ensured import directory exists"

# Copy required CSV files from demo_data to import directory
log "Copying citations and sentences files..."
cp $CITATIONS_FILE $IMPORT_CITATIONS
cp $SENTENCES_FILE $IMPORT_SENTENCES
log "Files copied successfully"

# Check for required files
log "Checking for required files..."
REQUIRED_FILES=(
    "$PREDICATION_FILE"
    "$PREDICATION_AUX_FILE"
    "$GENERIC_CONCEPT_FILE"
    "$ENTITY_FILE"
    "$CITATIONS_HEADER"
    "$SENTENCES_HEADER"
    "$ENTITIES_HEADER"
    "$PREDICATIONS_HEADER"
    "$IMPORT_CITATIONS"
    "$IMPORT_SENTENCES"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        log "ERROR: Required file '$file' not found in current directory"
        exit 1
    fi
    log "Found required file: $file"
done

# Join PREDICATION and PREDICATION_AUX
log "Joining predication files..."
awk -F',' 'NR==FNR{
    aux[$1] = $0;
    next
} 
($1 in aux) {
    split($0, pred, ",");
    split(aux[$1], aux_fields, ",");
    print pred[1] "," pred[2] "," pred[3] "," pred[4] "," pred[5] "," pred[6] "," \
          pred[7] "," pred[8] "," aux_fields[2] "," aux_fields[3] "," aux_fields[4] "," \
          aux_fields[5] "," aux_fields[6] "," aux_fields[7] "," aux_fields[8] "," \
          aux_fields[9] "," aux_fields[10] "," aux_fields[11] "," aux_fields[12] "," \
          aux_fields[13] "," aux_fields[14]
}' \
    $PREDICATION_AUX_FILE $PREDICATION_FILE > $MERGED_PREDICATIONS
log "Predication files joined successfully"

# Create a lookup for generic concepts and enrich entities
log "Creating lookup for generic concepts and enriching entities..."
awk -F',' 'NR>1 {generic_cuis[$2]=1; generic_scores[$2]=$6} END {
    while (getline < "'"$ENTITY_FILE"'") {
        split($0, fields, ",");
        if (fields[3] in generic_cuis) {
            print $0 ",true," generic_scores[fields[3]];
        } else {
            print $0 ",false,0";
        }
    }
}' $GENERIC_CONCEPT_FILE > $ENRICHED_ENTITIES
log "Entities enriched successfully"

# Neo4j admin is already in PATH in the container
NEO4J_ADMIN="neo4j-admin"

# Run Neo4j import
log "Starting Neo4j database import..."
"$NEO4J_ADMIN" database import full neo4j \
  --nodes=Citation="$CITATIONS_HEADER,$IMPORT_CITATIONS" \
  --nodes=Sentence="$SENTENCES_HEADER,$IMPORT_SENTENCES" \
  --nodes=Entity="$ENTITIES_HEADER,$ENRICHED_ENTITIES" \
  --relationships="$PREDICATIONS_HEADER,$MERGED_PREDICATIONS" \
  --delimiter="," \
  --array-delimiter=";" \
  --id-type=string \
  --skip-bad-relationships=true \
  --skip-duplicate-nodes=true \
  --overwrite-destination=true \
  --max-off-heap-memory=24G \
  --threads=8 \
  --verbose \
  --ignore-empty-strings=true \
  --bad-tolerance=100000 \
  --skip-bad-entries-logging=true

IMPORT_STATUS=$?
if [ $IMPORT_STATUS -eq 0 ]; then
    log "Import completed successfully"
else
    log "ERROR: Import failed with status $IMPORT_STATUS"
    exit $IMPORT_STATUS
fi

log "Import process finished. Check logs above for any errors."