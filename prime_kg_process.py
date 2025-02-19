import pandas as pd

# # Load the CSV data
# csv_file_path = 'data/kg.csv'
# data = pd.read_csv(csv_file_path, low_memory=False)

# # Create unique edges DataFrame with relation information
# unique_edges = data[['relation', 'display_relation', 'x_index', 'y_index']].copy()
# unique_edges_file_path = 'data/unique_edges.csv'
# unique_edges.to_csv(unique_edges_file_path, index=False)

# # Create unique nodes DataFrame
# # For x nodes
# x_nodes = data[['x_index', 'x_id', 'x_type', 'x_name']].drop_duplicates(subset=['x_index'])
# x_nodes.columns = ['index', 'node_id', 'node_type', 'node_name']

# # For y nodes
# y_nodes = data[['y_index', 'y_id', 'y_type', 'y_name']].drop_duplicates(subset=['y_index'])
# y_nodes.columns = ['index', 'node_id', 'node_type', 'node_name']

# # Combine x and y nodes and remove duplicates based on all columns
# unique_nodes = pd.concat([x_nodes, y_nodes])
# unique_nodes = unique_nodes.drop_duplicates(subset=['node_id', 'node_type', 'node_name'])
# unique_nodes_file_path = 'data/unique_nodes.csv'
# unique_nodes.to_csv(unique_nodes_file_path, index=False)

# # Print statistics
# print(f'Total relationships: {len(unique_edges)}')
# print(f'Total unique nodes: {len(unique_nodes)}')
# print(f'Unique node types: {unique_nodes["node_type"].unique()}')

# # Verification
# print('\nVerification counts by node type:')
# print(unique_nodes['node_type'].value_counts())

# Load the unique edges data
edges_file_path = 'data/unique_edges.csv'
edges_data = pd.read_csv(edges_file_path)

# Get and display relationship statistics
print("\nRelationship Type Analysis:")
print("-" * 50)

# Count unique relationships
relationship_counts = edges_data['relation'].value_counts()
display_relation_counts = edges_data['display_relation'].value_counts()

print(f"\nTotal unique relationship types: {len(relationship_counts)}")
print("\nRelationship counts:")
print("-" * 50)
for rel, count in relationship_counts.items():
    display_rel = edges_data[edges_data['relation'] == rel]['display_relation'].iloc[0]
    print(f"{rel} ({display_rel}): {count} occurrences")

# Save relationship statistics to CSV
relationship_stats = pd.DataFrame({
    'relation': relationship_counts.index,
    'count': relationship_counts.values
}).merge(
    edges_data[['relation', 'display_relation']].drop_duplicates(),
    on='relation'
)

stats_file_path = 'data/relationship_stats.csv'
relationship_stats.to_csv(stats_file_path, index=False)
print(f"\nDetailed statistics saved to: {stats_file_path}")