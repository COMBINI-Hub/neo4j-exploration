#!/usr/bin/env zsh

# Uninstall Neo4j chart and clean up PVCs
RELEASE_NAME=${1:-neo4j}

echo "🗑️  Uninstalling Neo4j Helm release..."
helm uninstall $RELEASE_NAME

echo "🧹 Cleaning up persistent volumes..."
kubectl delete pvc -l "app.kubernetes.io/instance=$RELEASE_NAME"

echo "✨ Cleanup completed!"