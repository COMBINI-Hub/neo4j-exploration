import logging
import os
from neo4j import GraphDatabase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SemMedDBLoader:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        logger.info("Connected to Neo4j database")

    def clear_database(self):
        """Clear all nodes and relationships in the database"""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        logger.info("Cleared all nodes and relationships from database")

    def create_constraints(self):
        """Create uniqueness constraints"""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Concept) REQUIRE c.cui IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Sentence) REQUIRE s.sentence_id IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Citation) REQUIRE c.pmid IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (g:GenericConcept) REQUIRE g.cui IS UNIQUE"
            ]
            for constraint in constraints:
                session.run(constraint)
        logger.info("Created database constraints")

    def load_generic_concepts(self, file_path: str):
        """Load generic concepts as nodes"""
        query = """
        LOAD CSV FROM 'file:///' + $file AS row
        MERGE (g:GenericConcept {cui: row[1]})
        SET g.name = row[2]
        """
        self._execute_load(file_path, query)
        logger.info("Loaded generic concepts")

    def load_concepts(self, file_path: str):
        """Load concepts data, excluding generic concepts"""
        query = """
        LOAD CSV WITH HEADERS FROM 'file:///' + $file AS row 
        FIELDTERMINATOR ',' QUOTE '"'
        WITH trim(row['0']) as id, trim(row['1']) as citation_id, trim(row['2']) as sentence_id, 
            trim(row['3']) as cui, trim(row['4']) as name, trim(row['5']) as type, 
            trim(row['9']) as score
        OPTIONAL MATCH (g:GenericConcept {cui: cui})
        WITH id, citation_id, sentence_id, cui, name, type, score, g
        WHERE g IS NULL
        MERGE (c:Concept {cui: cui})
        SET c.name = name,
            c.type = type,
            c.score = CASE WHEN score IS NULL THEN null ELSE toFloat(score) END
        """
        self._execute_load(file_path, query)
        logger.info("Loaded concepts data")

    def load_predications(self, pred_file: str, pred_aux_file: str):
        """Load predications data, excluding those involving generic concepts"""
        # First load main predications
        pred_query = """
        LOAD CSV FROM 'file:///' + $file AS row
        WITH row
        MATCH (subject:Concept {cui: row[1]})
        MATCH (object:Concept {cui: row[2]})
        OPTIONAL MATCH (g:GenericConcept)
        WHERE g.cui IN [row[1], row[2]]
        WITH row, subject, object, g
        WHERE g IS NULL
        CREATE (subject)-[r:PREDICATE {
            predicate_id: row[3],
            predicate: row[4],
            sentence_id: row[0]
        }]->(object)
        """
        self._execute_load(pred_file, pred_query)
        
        # Then load auxiliary information
        aux_query = """
        LOAD CSV FROM 'file:///' + $file AS row
        MATCH ()-[r:PREDICATE {predicate_id: row[0]}]->()
        SET r.subject_text = row[1],
            r.object_text = row[2],
            r.subject_score = toFloat(row[3]),
            r.object_score = toFloat(row[4]),
            r.type = row[5]
        """
        self._execute_load(pred_aux_file, aux_query)
        logger.info("Loaded predications data")

    def load_citations(self, file_path: str):
        """Load citations data"""
        query = """
        LOAD CSV FROM 'file:///' + $file AS row
        MERGE (c:Citation {pmid: row[0]})
        SET c.issn = row[1],
            c.pub_date = row[2],
            c.pub_year = row[4]
        """
        self._execute_load(file_path, query)
        logger.info("Loaded citations data")

    def load_sentences(self, file_path: str):
        """Load sentences data"""
        query = """
        LOAD CSV FROM 'file:///' + $file AS row
        MATCH (c:Citation {pmid: row[1]})
        MERGE (s:Sentence {sentence_id: row[0]})
        SET s.type = row[2],
            s.number = row[3],
            s.text = row[5]
        CREATE (c)-[:HAS_SENTENCE]->(s)
        """
        self._execute_load(file_path, query)
        logger.info("Loaded sentences data")

    def _execute_load(self, file_path: str, query: str):
        """Execute a LOAD CSV query"""
        filename = os.path.basename(file_path)
        
        # Add count of lines in file
        line_count = sum(1 for _ in open(file_path))
        logger.info(f"Total lines in {filename}: {line_count}")

        logger.info(f"Loading file: {file_path}")
        logger.info(f"First few lines of file:")
        with open(file_path, 'r') as f:
            for i, line in enumerate(f):
                if i < 3:  # Show first 3 lines
                    logger.info(line.strip())
                else:
                    break

        # Execute load
        with self.driver.session() as session:
            session.run(query, file=filename)

    def close(self):
        self.driver.close()
        logger.info("Closed Neo4j connection")

def main():
    # Configuration
    NEO4J_URI = "neo4j://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "your_password"
    DATA_DIR = "demo_data"

    try:
        # Initialize loader
        loader = SemMedDBLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

        loader.clear_database()

        # Create constraints
        loader.create_constraints()

        # Load generic concepts first
        loader.load_generic_concepts(os.path.join(DATA_DIR, "generic_concept.csv"))

        # Load data
        loader.load_citations(os.path.join(DATA_DIR, "citations.csv"))
        loader.load_concepts(os.path.join(DATA_DIR, "entity.csv"))
        loader.load_sentences(os.path.join(DATA_DIR, "sentence.csv"))
        loader.load_predications(
            os.path.join(DATA_DIR, "predication.csv"),
            os.path.join(DATA_DIR, "predication_aux.csv")
        )

        logger.info("Successfully completed loading SemMedDB data")

    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

    finally:
        loader.close()

if __name__ == "__main__":
    main()