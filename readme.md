# Knowledge Graph Creator

[WIP] This project creates a knowledge graph in Neo4j from the [HALD](https://figshare.com/articles/dataset/HALD_a_human_aging_and_longevity_knowledge_graph_for_precision_gerontology_and_geroscience_analyses/22828196) dataset.

## Prerequisites

- Python 3.6+
- Docker
- `neo4j` Python driver
- Helm 3.x
- Kubernetes 1.x
- kubectl configured to connect to your Kubernetes cluster

## Installation

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/knowledge-graph-creator.git
   cd knowledge-graph-creator
   ```

2. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

3. Deploy Neo4j using Helm:
   ```bash
   helm install neo4j ./neo4j-chart
   ```
   
   Or use the helper script:
   ```bash
   ./scripts/helm-install.sh
   ```

### Debugging

1. Run the debug script to get detailed information:
   ```zsh
   ./scripts/debug.zsh
   ```

2. Test Neo4j connectivity:
   ```zsh
   python scripts/test_connection.py [uri] [username] [password]
   ```
   
### Cleanup

To remove Neo4j and all associated resources:
```bash
./scripts/helm-uninstall.sh
```

### Common Issues and Solutions

1. Pod not starting:
   - Check pod events: `kubectl describe pod <pod-name>`
   - Verify PVC binding: `kubectl get pvc`
   - Check logs: `kubectl logs <pod-name>`

2. Connection issues:
   - Verify service is running: `kubectl get svc neo4j`
   - Check firewall rules
   - Ensure correct credentials in deployment

3. Data persistence issues:
   - Check PV/PVC status: `kubectl get pv,pvc`
   - Verify volume mounting: `kubectl describe pod <pod-name>`

## Usage

1. Update the `uri` variable in `test.py` with your Neo4j database URI.

2. Run the script:
   ```
   python test.py
   ```

3. The script will create the knowledge graph and print a summary of the created graph.

## File Descriptions

- `test.py`: Main script that reads CSV files and creates the knowledge graph in Neo4j.
- `data/entities.csv`: CSV file containing entity data.
- `data/roles.csv`: CSV file containing relationship data between entities.

## Functions

- `load_csv_data(file_path)`: Loads data from a CSV file.
- `sanitize_label(label)`: Sanitizes labels to be Neo4j-compliant.
- `create_knowledge_graph(tx, entities, roles)`: Creates nodes and relationships in Neo4j.
- `print_graph_summary(session)`: Prints a summary of the created graph.

## Output

The script will output:
1. Confirmation that the knowledge graph was created successfully.
2. Total number of nodes and relationships in the graph.
3. Sample of up to 5 nodes with their labels and properties.
4. Sample of up to 5 relationships with their types and properties.

## Troubleshooting

- Ensure your Neo4j database is running and accessible.
- Check that your CSV files match the expected schema.
- Verify that you have the necessary permissions to read the CSV files and write to the Neo4j database.
