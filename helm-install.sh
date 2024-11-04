#!/usr/bin/env zsh

# Install/upgrade Neo4j chart
helm upgrade --install neo4j ./neo4j-chart \
    --namespace default \
    --create-namespace \
    --wait

# Display connection information
echo "\nNeo4j has been deployed!"
echo "To connect to Neo4j Browser:"
kubectl get svc neo4j -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
echo ":7474"