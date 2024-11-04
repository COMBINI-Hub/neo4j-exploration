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

# Create k8s directory if it doesn't exist
if [[ ! -d "k8s" ]]; then
    log_warning "k8s directory not found. Creating directory structure..."
    mkdir -p k8s
fi

# Create k8s configuration files if they don't exist
log_status "Setting up Kubernetes configuration files..."

# Create PV configuration
cat > k8s/neo4j-pv.yaml << 'EOL'
apiVersion: v1
kind: PersistentVolume
metadata:
  name: neo4j-pv
spec:
  capacity:
    storage: 10Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: standard
  hostPath:
    path: /data/neo4j
EOL
log_status "Created neo4j-pv.yaml"

# Create PVC configuration
cat > k8s/neo4j-pvc.yaml << 'EOL'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: neo4j-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: standard
EOL
log_status "Created neo4j-pvc.yaml"

# Create Deployment configuration
cat > k8s/neo4j-deployment.yaml << 'EOL'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: neo4j
spec:
  replicas: 1
  selector:
    matchLabels:
      app: neo4j
  template:
    metadata:
      labels:
        app: neo4j
    spec:
      containers:
      - name: neo4j
        image: neo4j:5.13.0
        ports:
        - containerPort: 7474
          name: browser
        - containerPort: 7687
          name: bolt
        env:
        - name: NEO4J_AUTH
          value: neo4j/yourpassword
        - name: NEO4J_ACCEPT_LICENSE_AGREEMENT
          value: "yes"
        volumeMounts:
        - name: neo4j-data
          mountPath: /data
      volumes:
      - name: neo4j-data
        persistentVolumeClaim:
          claimName: neo4j-pvc
EOL
log_status "Created neo4j-deployment.yaml"

# Create Service configuration
cat > k8s/neo4j-service.yaml << 'EOL'
apiVersion: v1
kind: Service
metadata:
  name: neo4j
spec:
  selector:
    app: neo4j
  ports:
  - port: 7474
    name: browser
    targetPort: 7474
  - port: 7687
    name: bolt
    targetPort: 7687
  type: LoadBalancer
EOL
log_status "Created neo4j-service.yaml"

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    log_error "kubectl is not installed. Please install it first."
    exit 1
fi

# Create kubernetes resources
log_status "Creating Persistent Volume..."
kubectl apply -f k8s/neo4j-pv.yaml

log_status "Creating Persistent Volume Claim..."
kubectl apply -f k8s/neo4j-pvc.yaml

log_status "Creating Neo4j Deployment..."
kubectl apply -f k8s/neo4j-deployment.yaml

log_status "Creating Neo4j Service..."
kubectl apply -f k8s/neo4j-service.yaml

# Wait for deployment
log_status "Waiting for Neo4j deployment to be ready..."
if kubectl wait --for=condition=available --timeout=300s deployment/neo4j; then
    log_status "Neo4j deployment is ready!"
else
    log_error "Deployment timeout. Check pod status."
fi

# Get service details
log_status "Neo4j Service Details:"
kubectl get svc neo4j