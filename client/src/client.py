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
            
        # Wait a moment before verification
        import time
        time.sleep(2)  # Add a 2-second delay
            
        # Verify counts in a new session
        with self.driver.session() as session:
            node_counts = {
                'citations.csv': "MATCH (c:Citation) RETURN count(c) as count",
                'entity.csv': "MATCH (c:Concept) RETURN count(c) as count",
                'sentence.csv': "MATCH (s:Sentence) RETURN count(s) as count"
            }
            
            relation_counts = {
                'sentence.csv': "MATCH ()-[r:HAS_SENTENCE]->() RETURN count(r) as count",
                'predication.csv': "MATCH ()-[r:PREDICATE]->() RETURN count(r) as count"
            }
            
            if filename in node_counts:
                result = session.run(node_counts[filename])
                count = result.single()['count']
                logger.info(f"Created {count} nodes from {filename}")
                
            if filename in relation_counts:
                result = session.run(relation_counts[filename])
                count = result.single()['count']
                logger.info(f"Created {count} relationships from {filename}")

def check_index_consistency(files_dict: dict[str, tuple[str, list[str]]]):
    """
    Check index consistency between related files
    
    Args:
        files_dict: Dictionary mapping file paths to tuples of (column_index, related_files)
            where column_index is the position of the ID column to check
    """
    file_indices = {}
    
    # First, load all indices
    for file_path, (id_col, _) in files_dict.items():
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            continue
            
        indices = set()
        with open(file_path, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        index = line.split(',')[id_col].strip('"')
                        indices.add(index)
                    except IndexError:
                        continue
        
        file_indices[file_path] = indices
        logger.info(f"Loaded {len(indices)} indices from {os.path.basename(file_path)}")

    # Then check relationships
    for file_path, (_, related_files) in files_dict.items():
        current_indices = file_indices.get(file_path, set())
        
        for related_file in related_files:
            related_indices = file_indices.get(related_file, set())
            
            if not related_indices:
                continue

            # Find indices that exist in current file but not in related file
            missing_indices = current_indices - related_indices
            if missing_indices:
                logger.warning(
                    f"Found {len(missing_indices)} indices in {os.path.basename(file_path)} "
                    f"that are missing from {os.path.basename(related_file)}. "
                    f"First few missing indices: {list(missing_indices)[:5]}"
                )

def main():
    # Configuration
    NEO4J_URI = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
    DATA_DIR = os.getenv("DATA_DIR", "/var/lib/neo4j/import")

    # Define files and their relationships
    files = {
        os.path.join(DATA_DIR, "citations.csv"): (0, []),  # PMID is column 0
        os.path.join(DATA_DIR, "entity.csv"): (0, []),     # CUI is column 0
        os.path.join(DATA_DIR, "sentence.csv"): (1, [      # sentence_id is column 1
            os.path.join(DATA_DIR, "citations.csv"),        # references PMID
        ]),
        os.path.join(DATA_DIR, "predication.csv"): (0, [   # predication_id is column 0
            os.path.join(DATA_DIR, "sentence.csv"),        # references sentence_id
            os.path.join(DATA_DIR, "entity.csv"),         # references CUI (subject)
            os.path.join(DATA_DIR, "entity.csv"),         # references CUI (object)
        ]),
        os.path.join(DATA_DIR, "predication_aux.csv"): (0, [  # predication_id is column 0
            os.path.join(DATA_DIR, "predication.csv"),     # references predication_id
        ])
    }

    try:
        # Check data consistency first
        logger.info("Checking data consistency...")
        check_index_consistency(files)
        
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