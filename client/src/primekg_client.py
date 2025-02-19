from neo4j import GraphDatabase
import logging
import argparse
import os

# Total relationships: 8100498
# Total unique nodes: 129375
# Unique node types: ['gene/protein' 'drug' 'effect/phenotype' 'disease' 'biological_process'
#  'molecular_function' 'cellular_component' 'exposure' 'pathway' 'anatomy']

# Verification counts by node type:
# node_type
# biological_process    28642
# gene/protein          27671
# disease               17080
# effect/phenotype      15311
# anatomy               14035
# molecular_function    11169
# drug                   7957
# cellular_component     4176
# pathway                2516
# exposure                818
# Name: count, dtype: int64

def parse_args():
    parser = argparse.ArgumentParser(description='Load PrimeKG data into Neo4j database')
    parser.add_argument('--constraints', action='store_true', help='Create constraints and indexes')
    parser.add_argument('--nodes', action='store_true', help='Load nodes')
    parser.add_argument('--relationships', action='store_true', help='Create relationships')
    parser.add_argument('--all', action='store_true', help='Load everything')
    return parser.parse_args()

class Config:
    # Database connection
    NEO4J_URI = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    
    # Data file paths
    DATA_DIR = "data"  
    KG_FILE = f"{DATA_DIR}/kg.csv"
    
    # Batch sizes
    BATCH_SIZE = 10000

class Neo4jConnector:
    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)
        self.logger = self._setup_logger()
        # Verify connection
        self._verify_connection()
        # Verify file access
        self._verify_file_access()

    def _verify_connection(self):
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 1 as test")
                result.single()
                self.logger.info("Successfully connected to Neo4j database")
        except Exception as e:
            self.logger.error(f"Failed to connect to Neo4j database: {str(e)}")
            raise

    def _verify_file_access(self):
        try:
            if not os.path.exists(Config.KG_FILE):
                self.logger.error(f"File not found: {Config.KG_FILE}")
                raise FileNotFoundError(f"Cannot access {Config.KG_FILE}")
            
            # Read and print first few lines of the file
            with open(Config.KG_FILE, 'r') as f:
                header = f.readline().strip()
                first_line = f.readline().strip()
                self.logger.info("File access verified. Sample data:")
                self.logger.info(f"Header: {header}")
                self.logger.info(f"First line: {first_line}")
        except Exception as e:
            self.logger.error(f"Failed to access data file: {str(e)}")
            raise
        
    def _setup_logger(self):
        logger = logging.getLogger("PrimeKGLoader")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def close(self):
        self.driver.close()

    def create_constraints(self):
        constraints = [
            "CREATE CONSTRAINT node_id_constraint IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT gene_protein_id_constraint IF NOT EXISTS FOR (g:GeneProtein) REQUIRE g.id IS UNIQUE"
        ]
        
        indexes = [
            "CREATE INDEX node_type_index IF NOT EXISTS FOR (n:Node) ON (n.type)",
            "CREATE INDEX node_name_index IF NOT EXISTS FOR (n:Node) ON (n.name)",
            "CREATE INDEX node_source_index IF NOT EXISTS FOR (n:Node) ON (n.source)"
        ]
        
        with self.driver.session() as session:
            self.logger.info("Creating constraints...")
            for constraint in constraints:
                session.run(constraint)
            
            self.logger.info("Creating indexes...")
            for index in indexes:
                session.run(index)

    def load_nodes(self):
        # First pass: Create unique nodes
        create_nodes_query = """
            CALL apoc.periodic.iterate(
"LOAD CSV WITH HEADERS FROM 'file:///unique_nodes.csv' AS row RETURN row",
"CALL {
    WITH row
    FOREACH (dummy IN CASE WHEN row.node_type = 'gene/protein' THEN [1] ELSE [] END |
        CREATE (n:GeneProtein {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
    FOREACH (dummy IN CASE WHEN row.node_type = 'biological_process' THEN [1] ELSE [] END |
        CREATE (n:BiologicalProcess {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
    FOREACH (dummy IN CASE WHEN row.node_type = 'disease' THEN [1] ELSE [] END |
        CREATE (n:Disease {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
    FOREACH (dummy IN CASE WHEN row.node_type = 'effect/phenotype' THEN [1] ELSE [] END |
        CREATE (n:Phenotype {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
    FOREACH (dummy IN CASE WHEN row.node_type = 'anatomy' THEN [1] ELSE [] END |
        CREATE (n:Anatomy {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
    FOREACH (dummy IN CASE WHEN row.node_type = 'molecular_function' THEN [1] ELSE [] END |
        CREATE (n:MolecularFunction {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
    FOREACH (dummy IN CASE WHEN row.node_type = 'drug' THEN [1] ELSE [] END |
        CREATE (n:Drug {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
    FOREACH (dummy IN CASE WHEN row.node_type = 'cellular_component' THEN [1] ELSE [] END |
        CREATE (n:CellularComponent {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
    FOREACH (dummy IN CASE WHEN row.node_type = 'pathway' THEN [1] ELSE [] END |
        CREATE (n:Pathway {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
    FOREACH (dummy IN CASE WHEN row.node_type = 'exposure' THEN [1] ELSE [] END |
        CREATE (n:Exposure {id: row.node_id, name: row.node_name, index: toInteger(row.index)})
    )
}",
{batchSize: 1000, parallel: false}
);
        """
        
        with self.driver.session() as session:
            self.logger.info("Creating nodes...")
            session.run(create_nodes_query, 
                       file=Config.KG_FILE, 
                       batchSize=Config.BATCH_SIZE)
            
            count = self.get_node_count("Node")
            self.logger.info(f"Created {count} nodes")

    def create_relationships(self):
        create_relationships_query = """
        CALL apoc.periodic.iterate(
            "LOAD CSV WITH HEADERS FROM $file AS row RETURN row",
            "MATCH (x), (y)
            WHERE x.id = row.x_id AND y.id = row.y_id
            MERGE (x)-[r:INTERACTS_WITH]->(y)
            SET r.source = row.source",
            {batchSize: $batchSize, parallel: false}
        );
        """
        
        with self.driver.session() as session:
            self.logger.info("Creating relationships...")
            session.run(create_relationships_query, 
                       file=Config.KG_FILE, 
                       batchSize=Config.BATCH_SIZE)
            
            count = self.get_relationship_count("RELATES_TO")
            self.logger.info(f"Created {count} relationships")

    def get_node_count(self, label):
        with self.driver.session() as session:
            result = session.run(f"MATCH (n:{label}) RETURN count(n) as count")
            return result.single()["count"]

    def get_relationship_count(self, type=None):
        query = "MATCH ()-[r]->() RETURN count(r) as count" if type is None else \
                f"MATCH ()-[r:{type}]->() RETURN count(r) as count"
        with self.driver.session() as session:
            result = session.run(query)
            return result.single()["count"]

def main():
    uri = Config.NEO4J_URI
    connector = Neo4jConnector(uri)
    args = parse_args()
    
    try:
        run_all = args.all or not any([args.constraints, args.nodes, args.relationships])

        if run_all or args.constraints:
            connector.logger.info("Creating constraints...")
            connector.create_constraints()

        if run_all or args.nodes:
            connector.logger.info("Loading nodes...")
            connector.load_nodes()

        if run_all or args.relationships:
            connector.logger.info("Creating relationships...")
            connector.create_relationships()
        
        # Log final statistics
        connector.logger.info("=== Final Database Statistics ===")
        connector.logger.info(f"Total nodes: {connector.get_node_count('Node')}")
        connector.logger.info(f"Total relationships: {connector.get_relationship_count()}")

    except Exception as e:
        connector.logger.error(f"An error occurred: {str(e)}")
    finally:
        connector.close()

if __name__ == "__main__":
    main()