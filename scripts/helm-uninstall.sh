#!/usr/bin/env zsh

# Uninstall both charts
echo "Uninstalling kg-client chart..."
helm uninstall kg-client -n kg
echo "Uninstalling kg-server chart..."
helm uninstall kg-server -n kg

# Clean up PVCs
echo "Cleaning up persistent volume claims..."
kubectl delete pvc -l "app.kubernetes.io/instance=kg-server" -n kg

echo "✨ Cleanup completed!"