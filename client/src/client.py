import logging
import os
from neo4j import GraphDatabase
import csv
import re
import time
import shutil
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SemMedDBLoader:
    def __init__(self, uri: str, user: str, password: str, max_retries: int = 3):
        self.uri = uri
        self.max_retries = max_retries
        self.driver = self._create_driver()
        logger.info("Connected to Neo4j database")

    def _create_driver(self):
        """Create a new driver with appropriate configurations"""
        return GraphDatabase.driver(
            self.uri,
            max_connection_lifetime=3600,  # 1 hour
            max_connection_pool_size=50,
            connection_acquisition_timeout=300  # 5 minutes
        )

    def _get_session(self):
        """Get a new session, recreating the driver if necessary"""
        try:
            return self.driver.session()
        except Exception as e:
            logger.warning(f"Session creation failed: {str(e)}. Attempting to recreate driver...")
            self.driver.close()
            self.driver = self._create_driver()
            return self.driver.session()
          
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
                "CREATE CONSTRAINT IF NOT EXISTS FOR (g:GenericConcept) REQUIRE g.cui IS UNIQUE",
                "CREATE CONSTRAINT entity_id IF NOT EXISTS FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE"
            ]
            for constraint in constraints:
                session.run(constraint)
        logger.info("Created database constraints")

    def load_predications(self, predication_path: str, predication_aux_path: str):
            """Load predication relationships and their auxiliary data"""
            query = """
            CALL apoc.periodic.iterate(
            "CALL apoc.load.csv($file, {delimiter: ',', quoteChar: '\"'}) YIELD map 
            RETURN map",
            "MATCH (c1:Concept {cui: map[4]})
            MATCH (c2:Concept {cui: map[9]})
            CREATE (c1)-[r:PREDICATE {
                predication_id: map[0],
                sentence_id: map[1],
                pmid: map[2],
                predicate_type: map[3],
                subject_type: map[6],
                object_type: map[11]
            }]->(c2)",
            {batchSize:500, iterateList:true, parallel:false}
            )
            """
            self._execute_load(predication_path, query)

    def load_sentences(self, file_path: str):
        """Load sentences from CSV"""
        query = """
        CALL apoc.periodic.iterate(
        "CALL apoc.load.csv($file, {delimiter: ',', quoteChar: '\"'}) YIELD map 
        RETURN map",
        "CREATE (s:Sentence {
            sentence_id: map[0],
            pmid: map[1],
            type: map[2],
            number: map[3],
            offset: map[4],
            text: map[5],
            end: map[6]
        })",
        {batchSize:1000, iterateList:true, parallel:false}
        )
        """
        self._execute_load(file_path, query)

    def load_generic_concepts(self, file_path: str):
        """Load generic concepts from CSV"""
        query = """
        CALL apoc.periodic.iterate(
        "CALL apoc.load.csv($file, {delimiter: ',', quoteChar: '\"'}) YIELD map 
        RETURN map",
        "CREATE (c:GenericConcept {
            id: map[0],
            cui: map[1],
            name: map[2]
        })",
        {batchSize:1000, iterateList:true, parallel:false}
        )
        """
        self._execute_load(file_path, query)

    def load_citations(self, file_path: str):
        """Load citations from CSV"""
        query = """
        CALL apoc.periodic.iterate(
        "CALL apoc.load.csv($file, {delimiter: ',', quoteChar: '\"'}) YIELD map 
        RETURN map",
        "CREATE (c:Citation {
            pmid: map[0],
            issn: map[1],
            dp: map[2],
            edat: map[3],
            pyear: map[4]
        })",
        {batchSize:1000, iterateList:true, parallel:false}
        )
        """
        self._execute_load(file_path, query)

    def _execute_load(self, file_path: str, query: str, filename: str = None):
        """Execute a LOAD CSV query using APOC for better performance and error handling"""
        def _do_load(session, file_path, query, filename):
            try:
                if filename is None:
                    filename = os.path.basename(file_path)
                
                logger.info(f"Starting load operation for: {filename}")
                
                # Execute the APOC query
                result = session.run(query, {"file": f"file:///{filename}"})
                
                # Get summary statistics
                summary = result.consume()
                
                logger.info(f"\nLoad Statistics for {filename}:")
                logger.info(f"├─ Nodes created: {summary.counters.nodes_created}")
                logger.info(f"├─ Relationships created: {summary.counters.relationships_created}")
                logger.info(f"├─ Properties set: {summary.counters.properties_set}")
                logger.info("└─ Load completed successfully")

                # Verify data loading
                verification_queries = {
                    'citations.csv': ("MATCH (c:Citation) RETURN count(c) as count", "Citation nodes"),
                    'generic_concept.csv': ("MATCH (c:GenericConcept) RETURN count(c) as count", "Generic Concept nodes"),
                    'sentence.csv': ("MATCH (s:Sentence) RETURN count(s) as count", "Sentence nodes"),
                    'predication.csv': ("MATCH ()-[r:PREDICATE]->() RETURN count(r) as count", "Predicate relationships")
                }

                if filename in verification_queries:
                    query, label = verification_queries[filename]
                    count_result = session.run(query)
                    count = count_result.single()['count']
                    logger.info(f"Verified {count:,} {label} after loading")

            except Exception as e:
                logger.error(f"\nError during load operation for {filename}")
                logger.error(f"Error details: {str(e)}")
                raise

        return self._execute_with_retry(_do_load, file_path, query, filename)
    
    def _execute_with_retry(self, operation, *args, **kwargs):
        """Execute a database operation with retries"""
        retries = 0
        last_exception = None

        while retries < self.max_retries:
            try:
                with self._get_session() as session:
                    return operation(session, *args, **kwargs)
            except Exception as e:
                last_exception = e
                retries += 1
                logger.warning(f"Attempt {retries} failed: {str(e)}")
                if retries < self.max_retries:
                    sleep_time = 2 ** retries  # Exponential backoff
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    # Recreate driver before retry
                    self.driver.close()
                    self.driver = self._create_driver()

        logger.error(f"Failed after {self.max_retries} attempts")
        raise last_exception
    
def close(self):
    self.driver.close()
    logger.info("Closed Neo4j connection")

def main():
    # Configuration
    NEO4J_URI = "neo4j://localhost:7687"
    NEO4J_USER = "neo4j"
    NEO4J_PASSWORD = "your_password"
    DEMO_DATA_DIR = "demo_data"
    DATA_DIR = "data"
    try:
        # Initialize loader
        loader = SemMedDBLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

        loader.clear_database()

        # Create constraints
        loader.create_constraints()

        # Load generic concepts first
        loader.load_generic_concepts(os.path.join(DEMO_DATA_DIR, "generic_concept.csv"))

        # Load data
        loader.load_citations(os.path.join(DEMO_DATA_DIR, "citations.csv"))
        loader.load_sentences(os.path.join(DEMO_DATA_DIR, "sentence.csv"))
        loader.load_predications(
            os.path.join(DEMO_DATA_DIR, "predication.csv"),
            os.path.join(DEMO_DATA_DIR, "predication_aux.csv")
        )
        loader.load_entities(os.path.join(DATA_DIR, "semmedVER43_2024_R_ENTITY.csv.gz"))

        logger.info("Successfully completed loading SemMedDB data")

    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

    finally:
        loader.close()

if __name__ == "__main__":
    main()