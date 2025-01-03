import logging
import os
from neo4j import GraphDatabase
import csv
import re
import time
import shutil
from tqdm import tqdm
from csv_preprocessor import CSVPreprocessor

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
    
    def preprocess_files(data_dir: str):
        """Preprocess all CSV files before loading into Neo4j"""
        logger.info("Starting preprocessing of all data files...")
        
        # List of files to preprocess (excluding .gz files)
        files_to_process = [
            "generic_concept.csv",
            "citations.csv",
            "sentence.csv",
            "predication.csv",
            "predication_aux.csv"
        ]
        
        preprocessor = CSVPreprocessor()
        
        for filename in files_to_process:
            file_path = os.path.join(data_dir, filename)
            if os.path.exists(file_path):
                try:
                    logger.info(f"Preprocessing {filename}...")
                    preprocessor.preprocess_csv(file_path)
                except Exception as e:
                    logger.error(f"Error preprocessing {filename}: {str(e)}")
                    raise
        
        logger.info("Completed preprocessing of all files")

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

    def load_generic_concepts(self, file_path: str):
        """Load generic concepts as nodes"""
        query = """
        USING PERIODIC COMMIT 500
        LOAD CSV FROM 'file:///' + $file AS row
        WITH 
            toInteger(trim(row[0])) as id,
            trim(row[1]) as cui,
            trim(row[2]) as name
        WHERE cui IS NOT NULL AND cui <> ''
        MERGE (g:GenericConcept {cui: cui})
        SET 
            g.id = id,
            g.name = CASE 
                WHEN name IS NOT NULL AND size(name) > 0 
                THEN name 
                ELSE '' 
            END
        """
        self._execute_load(file_path, query)
        logger.info("Loaded generic concepts")

    def load_entities(self, file_path: str):
            """Load entities data from gzipped CSV"""
            query = """
            LOAD CSV FROM 'file:///' + $file AS row
            MERGE (e:Entity {entity_id: toInteger(row[2])})
            ON CREATE SET 
                e.pmid = toInteger(row[0]),
                e.sentence_id = toInteger(row[1]),
                e.cui = row[3],
                e.name = row[4],
                e.type = row[5],
                e.start_index = toInteger(row[6]),
                e.end_index = toInteger(row[7]),
                e.score = toFloat(row[8])
            """
            self._execute_load(file_path, query)
            logger.info("Loaded entities data")

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
        CALL {
            LOAD CSV FROM 'file:///' + $file AS row
            MATCH ()-[r:PREDICATE {predicate_id: row[0]}]->()
            SET r.subject_text = row[1],
                r.object_text = row[2],
                r.subject_score = toFloat(row[3]),
                r.object_score = toFloat(row[4]),
                r.type = row[5]
        } IN TRANSACTIONS OF 10000 ROWS
        """
        self._execute_load(pred_aux_file, aux_query)
        logger.info("Loaded predications data")

    def load_citations(self, file_path: str):
        """Load citations data in batches"""
        query = """
        LOAD CSV FROM 'file:///' + $file AS row
            WITH row
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

    def _execute_load(self, file_path: str, query: str, filename: str = None):
        """Execute a LOAD CSV query using APOC periodic iterate for better memory management."""
        def _do_load(session, file_path, query, filename):
            try:
                if filename is None:
                    filename = os.path.basename(file_path)
                
                logger.info(f"Starting load operation for: {filename}")
                
                # Convert query for APOC
                data_query = f"LOAD CSV FROM 'file:///{filename}' AS row RETURN row"
                operation_query = query
                if "CALL {" in operation_query:
                    operation_query = operation_query.replace("CALL {", "").replace("} IN TRANSACTIONS OF 10000 ROWS", "")
                operation_query = operation_query.replace("LOAD CSV FROM 'file:///' + $file AS row", "WITH $_batch AS row")
                
                # Execute with progress monitoring
                progress_query = """
                CALL apoc.periodic.iterate(
                    $data_query,
                    $operation_query,
                    {
                        batchSize: 500,
                        parallel: false,
                        iterateList: true,
                        retries: 3,
                        batchMode: "BATCH_SINGLE",
                        params: {file: $filename},
                        reportInterval: 1000
                    }
                )
                YIELD batches, total, committedOperations, failedOperations, 
                      failedBatches, retries, errorMessages, batch, operations
                RETURN batches, total, committedOperations, failedOperations, 
                      failedBatches, retries, errorMessages
                """

                result = session.run(
                    progress_query,
                    {
                        'data_query': data_query,
                        'operation_query': operation_query,
                        'filename': filename
                    }
                )
                
                # Process results with progress monitoring
                stats = result.single()
                if stats:
                    logger.info(f"\nLoad Statistics for {filename}:")
                    logger.info(f"├─ Total rows processed: {stats['total']:,}")
                    logger.info(f"├─ Batches completed: {stats['batches']:,}")
                    logger.info(f"├─ Operations committed: {stats['committedOperations']:,}")
                    
                    if stats['failedOperations'] > 0:
                        logger.warning(f"├─ Failed operations: {stats['failedOperations']:,}")
                        logger.warning(f"├─ Failed batches: {stats['failedBatches']:,}")
                        logger.warning(f"├─ Retries performed: {stats['retries']:,}")
                        logger.warning(f"└─ Error messages: {stats['errorMessages']}")
                        
                        if stats['failedOperations'] > stats['committedOperations'] * 0.01:
                            raise Exception(
                                f"High failure rate detected: {stats['failedOperations']} "
                                f"failed operations out of {stats['total']} total operations"
                            )
                    else:
                        logger.info("└─ No failures reported")

                # Verify data loading
                verification_queries = {
                    'citations.csv': ("MATCH (c:Citation) RETURN count(c) as count", "Citation nodes"),
                    'entity.csv': ("MATCH (c:Concept) RETURN count(c) as count", "Concept nodes"),
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
        loader.load_generic_concepts(os.path.join(DATA_DIR, "generic_concept.csv"))

        # Load data
        loader.load_citations(os.path.join(DATA_DIR, "citations.csv"))
        loader.load_sentences(os.path.join(DATA_DIR, "sentence.csv"))
        loader.load_predications(
            os.path.join(DATA_DIR, "predication.csv"),
            os.path.join(DATA_DIR, "predication_aux.csv")
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