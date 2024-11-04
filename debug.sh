#!/usr/bin/env zsh

# Colors for status messages
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_status() {
    echo "${GREEN}[✓]${NC} $1"
}

log_warning() {
    echo "${YELLOW}[!]${NC} $1"
}

log_error() {
    echo "${RED}[✗]${NC} $1"
}

# Get pod name
POD_NAME=$(kubectl get pods -l app=neo4j -o jsonpath="{.items[0].metadata.name}")

if [[ -z "$POD_NAME" ]]; then
    log_error "No Neo4j pod found"
    exit 1
fi

log_status "Found Neo4j pod: $POD_NAME"

# Check pod status
log_status "Checking Pod Status..."
kubectl get pod $POD_NAME -o wide

log_status "Fetching Pod Logs..."
kubectl logs $POD_NAME

log_status "Getting Pod Description..."
kubectl describe pod $POD_NAME

# Check Neo4j connectivity
log_warning "Testing Neo4j Connection..."
if kubectl exec $POD_NAME -- cypher-shell -u neo4j -p yourpassword "MATCH (n) RETURN count(n) as count"; then
    log_status "Neo4j connection successful!"
else
    log_error "Failed to connect to Neo4j"
fi