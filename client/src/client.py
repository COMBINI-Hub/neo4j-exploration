import logging
import os
from neo4j import GraphDatabase
import csv
import re
import time

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

    def _preprocess_csv(self, input_path: str, output_path: str = None):
        """
        Preprocess CSV file to handle quote escaping issues.
        """
        output_path = output_path or f"{os.path.splitext(input_path)[0]}_processed{os.path.splitext(input_path)[1]}"
        
        try:
            with open(input_path, 'r', encoding='utf-8') as infile, \
                open(output_path, 'w', encoding='utf-8', newline='') as outfile:
                
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONE, escapechar='\\')
                for line_num, row in enumerate(csv.reader(infile), 1):
                    try:
                        # Remove quotes and whitespace from each field
                        cleaned_row = [field.strip().replace('"', '').replace("'", '') 
                                    for field in row if field is not None]
                        writer.writerow(cleaned_row)
                    except Exception as e:
                        logger.error(f"Error at line {line_num}: {row}")
                        raise
                        
            logger.info(f"Preprocessed CSV: {input_path} -> {output_path}")
            return output_path
                
        except Exception as e:
            logger.error(f"Failed to preprocess {input_path} at line {line_num if 'line_num' in locals() else 'unknown'}")
            raise
        """
        Preprocess CSV file to handle quote escaping issues.
        """
        if output_path is None:
            base, ext = os.path.splitext(input_path)
            output_path = f"{base}_processed{ext}"
        
        try:
            with open(input_path, 'r', encoding='utf-8') as infile, \
                open(output_path, 'w', encoding='utf-8', newline='') as outfile:
                
                reader = csv.reader(infile)
                writer = csv.writer(outfile, 
                                quoting=csv.QUOTE_MINIMAL,
                                escapechar='\\')
                
                for line_num, row in enumerate(reader, 1):
                    # Strip quotes and whitespace from each field
                    cleaned_row = [
                        re.sub(r'["\']', '', field.strip()) if field is not None else ''
                        for field in row
                    ]
                    try:
                        writer.writerow(cleaned_row)
                    except Exception as e:
                        logger.error(f"Error writing line {line_num}: {cleaned_row}")
                        logger.error(f"Original row: {row}")
                        raise
                        
            logger.info(f"Successfully preprocessed CSV file: {input_path} -> {output_path}")
            return output_path
                
        except Exception as e:
            logger.error(f"Error preprocessing CSV file {input_path} at line {line_num if 'line_num' in locals() else 'unknown'}")
            raise

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
        WITH 
            COALESCE(trim(row[0]), '') as id,
            COALESCE(trim(row[1]), '') as cui,
            COALESCE(trim(row[2]), '') as name
        WHERE cui <> ''
        MERGE (g:GenericConcept {cui: cui})
        SET g.name = CASE WHEN name <> '' THEN name ELSE '' END
        """
        self._execute_load(file_path, query)
        logger.info("Loaded generic concepts")

    def load_concepts(self, file_path: str):
        """Load concepts data, excluding generic concepts"""
        query = """
        LOAD CSV WITH HEADERS FROM 'file:///' + $file AS row 
        FIELDTERMINATOR ','
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

    def _execute_load(self, file_path: str, query: str, filename: str = None):
        """Execute a LOAD CSV query"""
        try:
            # Preprocess the CSV file and get the processed file path
            processed_file_path = self._preprocess_csv(file_path)
            
            # Use the filename from the processed path if none provided
            if filename is None:
                filename = os.path.basename(processed_file_path)
            
            line_count = sum(1 for _ in open(processed_file_path))
            logger.info(f"Total lines in {filename}: {line_count}")
            
            logger.info(f"Loading file: {processed_file_path}")
            logger.info(f"First few lines of file:")
            with open(processed_file_path, 'r') as f:
                for i, line in enumerate(f):
                    if i < 3:  # Show first 3 lines
                        logger.info(line.strip())
                    else:
                        break

            # Execute load with processed file
            with self.driver.session() as session:
                try:
                    session.run(query, file=filename)
                except Exception as e:
                    # Read the processed file to find problematic content
                    with open(processed_file_path, 'r') as f:
                        lines = f.readlines()
                        
                    logger.error(f"\nError executing query for {filename}:")
                    logger.error(f"Query: {query}")
                    logger.error(f"Total lines in file: {len(lines)}")
                    logger.error("Sample of file content:")
                    for i in range(min(5, len(lines))):
                        logger.error(f"Line {i+1}: {lines[i].strip()}")
                    raise
                
            # Wait a moment before verification
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
                    
        except Exception as e:
            # Extract position from error message if it exists
            import re
            position_match = re.search(r'position (\d+)', str(e))
            if position_match:
                position = int(position_match.group(1))
                
                # Read the processed file and find the problematic line
                with open(processed_file_path, 'r') as f:
                    content = f.read()
                    # Find the line number by counting newlines up to the position
                    line_number = content[:position].count('\n') + 1
                    
                    # Get the problematic line and surrounding context
                    lines = content.split('\n')
                    start_line = max(0, line_number - 2)
                    end_line = min(len(lines), line_number + 2)
                    
                    logger.error(f"\nError context for {filename}:")
                    logger.error(f"Error at position {position}, around line {line_number}")
                    logger.error("Surrounding lines:")
                    for i in range(start_line, end_line):
                        prefix = ">>> " if i + 1 == line_number else "    "
                        logger.error(f"{prefix}Line {i + 1}: {lines[i]}")
                        
            raise

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