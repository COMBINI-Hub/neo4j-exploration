from neo4j import GraphDatabase
import csv
import re

uri = "neo4j://localhost:7687" 

def load_csv_data(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)

def sanitize_label(label):
    sanitized = re.sub(r'[^\w]', '_', label)
    if sanitized[0].isdigit():
        sanitized = 'L_' + sanitized
    return sanitized

def create_knowledge_graph(tx, entities, roles):
    for entity in entities:
        labels = [sanitize_label(label.strip()) for label in entity[':LABEL'].split(';')]
        label_string = ':'.join(labels)
        tx.run(f"MERGE (e:Entity {{id: $id}}) "
               f"SET e.name = $name, e.type = $type, e.frequency = $frequency, "
               f"e:{label_string}",
               id=entity['entity:ID'], name=entity['name'], type=entity['type'],
               frequency=entity['frequency'])
    
    for role in roles:
        tx.run(f"MATCH (e1:Entity {{id: $entity1_id}}), (e2:Entity {{id: $entity2_id}}) "
               f"MERGE (e1)-[r:{sanitize_label(role[':TYPE'])} "
               f"{{relation: $relation, weight: $weight, method: $method}}]->(e2)",
               entity1_id=role[':START_ID'], entity2_id=role[':END_ID'],
               relation=role['relation'], weight=role['weight'],
               method=role['method'])

def print_graph_summary(session):
    # Count nodes
    result = session.run("MATCH (n) RETURN count(n) as node_count")
    node_count = result.single()["node_count"]
    print(f"Total nodes: {node_count}")

    # Count relationships
    result = session.run("MATCH ()-[r]->() RETURN count(r) as rel_count")
    rel_count = result.single()["rel_count"]
    print(f"Total relationships: {rel_count}")

    # Print sample nodes
    print("\nSample nodes:")
    result = session.run("MATCH (n) RETURN n LIMIT 5")
    for record in result:
        node = record["n"]
        print(f"Node: {node.labels} - Properties: {dict(node)}")

    # Print sample relationships
    print("\nSample relationships:")
    result = session.run("MATCH ()-[r]->() RETURN type(r) as type, r LIMIT 5")
    for record in result:
        rel_type = record["type"]
        rel = record["r"]
        print(f"Relationship: {rel_type} - Properties: {dict(rel)}")

entities = load_csv_data('data/entities.csv')
roles = load_csv_data('data/roles.csv')

with GraphDatabase.driver(uri) as driver:
    with driver.session() as session:
        session.execute_write(create_knowledge_graph, entities, roles)
        print("Knowledge graph created successfully!")
        print("\nGraph Summary:")
        print_graph_summary(session)
