import logging
import os
from neo4j import GraphDatabase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SemMedDBLoader:
    def __init__(self, uri: str):
        self.driver = GraphDatabase.driver(uri)
        logger.info("Connected to Neo4j database")

    def close(self):
        self.driver.close()
        logger.info("Closed Neo4j connection")

    def create_constraints(self):
        """Create uniqueness constraints"""
        with self.driver.session() as session:
            constraints = [
                "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Concept) REQUIRE c.cui IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Sentence) REQUIRE s.sentence_id IS UNIQUE",
                "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Citation) REQUIRE c.pmid IS UNIQUE"
            ]
            for constraint in constraints:
                session.run(constraint)
        logger.info("Created database constraints")

    def load_citations(self, file_path: str):
        """Load citations data"""
        query = """
        LOAD CSV FROM 'file:///' + $file AS row
        MERGE (c:Citation {pmid: trim(row[0])})
        SET c.issn = trim(row[1]),
            c.pub_date = trim(row[2]),
            c.pub_date_formatted = trim(row[3]),
            c.pub_year = trim(row[4])
        """
        filename = os.path.basename(file_path)
        self._execute_load(file_path, query, filename)
        logger.info("Loaded citations data")

    def load_concepts(self, file_path: str):
        """Load concepts from entity.csv"""
        query = """
        LOAD CSV FROM 'file:///' + $file AS row
        MERGE (c:Concept {cui: row[0]})
        SET c.name = row[1],
            c.type = row[2],
            c.score = toInteger(row[3])
        """
        filename = os.path.basename(file_path)
        self._execute_load(file_path, query, filename)
        logger.info("Loaded concept data")

    def load_sentences(self, file_path: str):
        """Load sentences data"""
        query = """
        LOAD CSV FROM 'file:///' + $file AS row
        MATCH (c:Citation {pmid: row[0]})
        MERGE (s:Sentence {sentence_id: row[1]})
        SET s.type = row[2],
            s.number = row[3],
            s.text = row[4]
        CREATE (c)-[:HAS_SENTENCE]->(s)
        """
        filename = os.path.basename(file_path)
        self._execute_load(file_path, query, filename)
        logger.info("Loaded sentences data")

    def load_predications(self, pred_file: str, pred_aux_file: str):
        """Load predications and their auxiliary information"""
        # First load main predications
        pred_query = """
        LOAD CSV FROM 'file:///' + $file AS row
        MATCH (s:Sentence {sentence_id: row[0]})
        MATCH (subject:Concept {cui: row[1]})
        MATCH (object:Concept {cui: row[2]})
        CREATE (subject)-[r:PREDICATE {
            predicate_id: row[3],
            predicate: row[4],
            sentence_id: row[0]
        }]->(object)
        """
        pred_filename = os.path.basename(pred_file)
        self._execute_load(pred_file, pred_query, pred_filename)
        
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
        pred_aux_filename = os.path.basename(pred_aux_file)           
        self._execute_load(pred_aux_file, aux_query, pred_aux_filename)
        logger.info("Loaded predications data")

    def _execute_load(self, file_path: str, query: str, filename: str = None):
        """Execute a LOAD CSV query"""
        if filename is None:
            filename = os.path.basename(file_path)
            
        logger.info(f"Loading file: {file_path}")
        logger.info(f"First few lines of file:")
        with open(file_path, 'r') as f:
            for i, line in enumerate(f):
                if i < 3:  # Show first 3 lines
                    logger.info(line.strip())
                else:
                    break
        with self.driver.session() as session:
            session.run(query, file=filename)

def main():
    # Configuration
    NEO4J_URI = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    DATA_DIR = os.getenv("DATA_DIR", "/var/lib/neo4j/import")

    try:
        # Initialize loader
        loader = SemMedDBLoader(NEO4J_URI)
        
        # Create constraints
        loader.create_constraints()

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