#!/usr/bin/env zsh

echo "🧹 Starting cleanup process..."

# Kubernetes cleanup
if command -v kubectl &> /dev/null; then
    echo "📦 Cleaning up Kubernetes resources..."
    kubectl delete deployment neo4j --ignore-not-found
    kubectl delete service neo4j --ignore-not-found
    kubectl delete pvc neo4j-pvc --ignore-not-found
    kubectl delete pv neo4j-pv --ignore-not-found
fi

# Docker cleanup
if command -v docker &> /dev/null; then
    echo "🐳 Cleaning up Docker resources..."
    # Stop and remove Neo4j container
    docker stop $(docker ps -q --filter name=neo4j) 2>/dev/null || true
    docker rm $(docker ps -aq --filter name=neo4j) 2>/dev/null || true
    
    # Remove Neo4j volumes
    docker volume rm $(docker volume ls -q --filter name=neo4j) 2>/dev/null || true
fi

# Clean up data directory
if [ -d "$HOME/neo4j/data" ]; then
    echo "🗑️  Removing local Neo4j data directory..."
    rm -rf "$HOME/neo4j/data"
fi

# Clean up any temporary files
echo "🧽 Cleaning up temporary files..."
find . -type f -name "*.log" -delete
find . -type f -name "*.tmp" -delete
find . -type f -name ".DS_Store" -delete

echo "✨ Cleanup completed successfully!"