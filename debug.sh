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

# Get release name
RELEASE_NAME=${1:-neo4j}

# Check if helm release exists
if ! helm status $RELEASE_NAME &> /dev/null; then
    log_error "Helm release '$RELEASE_NAME' not found"
    exit 1
fi

log_status "Checking Helm Release Status..."
helm status $RELEASE_NAME

# Get pod name using helm labels
POD_NAME=$(kubectl get pods -l "app.kubernetes.io/name=neo4j-chart,app.kubernetes.io/instance=$RELEASE_NAME" -o jsonpath="{.items[0].metadata.name}")

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
if kubectl exec $POD_NAME -- cypher-shell -u neo4j -p "$(helm get values $RELEASE_NAME -a -o json | jq -r '.neo4j.auth.password')" "MATCH (n) RETURN count(n) as count"; then
    log_status "Neo4j connection successful!"
else
    log_error "Failed to connect to Neo4j"
fi

# Show Helm values
log_status "Current Helm Values:"
helm get values $RELEASE_NAME -a