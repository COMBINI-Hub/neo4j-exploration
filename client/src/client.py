from neo4j import GraphDatabase
import logging
import time

# Configuration
class Config:
    # Database connection
    NEO4J_URI = "neo4j://localhost:7687"
    
    # Data file paths
    DATA_DIR = "data"
    CITATIONS_FILE = f"{DATA_DIR}/citations.csv"
    SENTENCES_FILE = f"{DATA_DIR}/sentences.csv"
    ENTITIES_FILE = f"{DATA_DIR}/entity.gz"
    PREDICATIONS_FILE = f"{DATA_DIR}/predications.csv"
    
    # Batch sizes for different operations
    CITATION_BATCH_SIZE = 1000
    SENTENCE_BATCH_SIZE = 1000
    ENTITY_BATCH_SIZE = 1000
    PREDICATION_BATCH_SIZE = 1000
    RELATIONSHIP_BATCH_SIZE = 500
class Neo4jConnector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.logger = self._setup_logger()
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
    def _setup_logger(self):
        logger = logging.getLogger("Neo4jLoader")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def close(self):
        self.driver.close()

    def create_constraints(self):
        # Constraints
        constraints = [
            # Primary key constraints
            "CREATE CONSTRAINT entity_id_primary IF NOT EXISTS FOR (e:Entity) REQUIRE e.ENTITY_ID IS UNIQUE",
            "CREATE CONSTRAINT predication_id_constraint IF NOT EXISTS FOR (p:Predication) REQUIRE p.predication_id IS UNIQUE"
        ]
        
        # Indexes
        indexes = [
            # Entity indexes
            "CREATE INDEX entity_sentence_id IF NOT EXISTS FOR (e:Entity) ON (e.SENTENCE_ID)",
            "CREATE INDEX pmid_entity_index_btree IF NOT EXISTS FOR (e:Entity) ON (e.PMID, e.START_INDEX)",
            
            # Sentence indexes
            "CREATE INDEX sentence_text IF NOT EXISTS FOR (s:Sentence) ON (s.SENTENCE)",
            "CREATE INDEX sentence_pmid_index IF NOT EXISTS FOR (s:Sentence) ON (s.number)",
            
            # Generic Concept index
            "CREATE INDEX generic_concept_name IF NOT EXISTS FOR (g:GenericConcept) ON (g.name)",
            
            # Predication indexes
            "CREATE INDEX predication_sentence_id IF NOT EXISTS FOR (p:Predication) ON (p.SENTENCE_ID)",
            "CREATE INDEX predication_id_range IF NOT EXISTS FOR (p:Predication) ON (p.predication_id)",
            "CREATE POINT INDEX predication_id_point IF NOT EXISTS FOR (p:Predication) ON (p.predication_id)"
        ]
        
        with self.driver.session() as session:
            self.logger.info("Creating constraints...")
            for constraint in constraints:
                session.run(constraint)
            
            self.logger.info("Creating indexes...")
            for index in indexes:
                session.run(index)
            
            result = session.run("SHOW CONSTRAINTS")
            constraint_count = len(list(result))
            result = session.run("SHOW INDEXES")
            index_count = len(list(result))
            self.logger.info(f"Total constraints: {constraint_count}, Total indexes: {index_count}")

    def load_citations(self):
        query = """
        CALL apoc.periodic.iterate(
            'CALL apoc.load.csv($file, {separator:",", header:false}) YIELD list 
             RETURN trim(list[0]) as pmid,
                    trim(list[1]) as issn,
                    trim(list[2]) as dp,
                    trim(list[3]) as edat,
                    trim(list[4]) as pyear',
            'CREATE (c:Citation {
                pmid: pmid,
                issn: issn,
                dp: dp,
                edat: edat,
                pyear: pyear
            })',
            {batchSize: $batchSize, iterateList:true, parallel:false, params: {file: $file}}
        )
        """
        with self.driver.session() as session:
            session.run(query, file=Config.CITATIONS_FILE, batchSize=Config.CITATION_BATCH_SIZE)
            count = self.get_node_count("Citation")
            self.logger.info(f"Citations in database: {count}")
            
    def load_sentences(self):
        query = """
        CALL apoc.periodic.iterate(
            'CALL apoc.load.csv($file, {separator:",", header:false}) YIELD list 
             RETURN trim(list[0]) as sentence_id,
                    trim(list[1]) as pmid,
                    trim(list[2]) as type,
                    trim(list[3]) as number,
                    trim(list[4]) as sent_start_index,
                    trim(list[5]) as sent_end_index,
                    trim(list[6]) as section_header,
                    trim(list[7]) as normalized_section_header,
                    trim(list[8]) as sentence',
            'CREATE (s:Sentence {
                sentence_id: sentence_id,
                pmid: pmid,
                type: type,
                number: number,
                sent_start_index: sent_start_index,
                sent_end_index: sent_end_index,
                section_header: section_header,
                normalized_section_header: normalized_section_header,
                sentence: sentence
            })',
            {batchSize: $batchSize, iterateList:true, parallel:false, params: {file: $file}}
        )
        """
        with self.driver.session() as session:
            session.run(query, file=Config.SENTENCES_FILE, batchSize=Config.SENTENCE_BATCH_SIZE)
            count = self.get_node_count("Sentence")
            self.logger.info(f"Sentences in database: {count}")
            
    def load_entities(self):
        query = """
        CALL apoc.periodic.iterate(
            'CALL apoc.load.csv($file, {
                compression: "GZIP", 
                separator:",", 
                header:false
            }) 
            YIELD list 
            RETURN trim(list[0]) as entity_id,
                   trim(list[1]) as sentence_id,
                   trim(list[2]) as cui,
                   trim(list[3]) as name,
                   trim(list[4]) as type,
                   trim(list[5]) as gene_id,
                   trim(list[6]) as gene_name,
                   trim(list[7]) as text,
                   trim(list[8]) as start_index,
                   trim(list[9]) as end_index,
                   trim(list[10]) as score',
            'CREATE (e:Entity {
                entity_id: entity_id,
                sentence_id: sentence_id,
                cui: cui,
                name: name,
                type: type,
                gene_id: gene_id,
                gene_name: gene_name,
                text: text,
                start_index: start_index,
                end_index: end_index,
                score: score
            })',
            {
                batchSize: $batchSize,
                iterateList: true,
                parallel: false,
                params: {file: $file},
                concurrency: 1
            }
        )
        """
        with self.driver.session() as session:
            session.run(query, file=Config.ENTITIES_FILE, batchSize=Config.ENTITY_BATCH_SIZE)
            count = self.get_node_count("Entity")
            self.logger.info(f"Entities in database: {count}")
            
    def create_relationships(self):
        relationships = [
            ("HAS_ENTITY", """
            CALL apoc.periodic.iterate(
                "MATCH (s:Sentence)
                 MATCH (e:Entity)
                 WHERE s.sentence_id = e.sentence_id
                 RETURN s, e",
                "CREATE (s)-[:HAS_ENTITY]->(e)",
                {batchSize: $batchSize}
            )
            """),
            ("HAS_PREDICATION", """
            CALL apoc.periodic.iterate(
                "MATCH (s:Sentence)
                 MATCH (p:Predication)
                 WHERE s.sentence_id = p.sentence_id
                 RETURN s, p",
                "CREATE (s)-[:HAS_PREDICATION]->(p)",
                {batchSize: $batchSize}
            )
            """),
            ("BELONGS_TO_SAME_CITATION", """
            CALL apoc.periodic.iterate(
                "MATCH (s:Sentence)
                 MATCH (p:Predication)
                 WHERE s.pmid = p.pmid
                 RETURN s, p",
                "CREATE (s)-[:BELONGS_TO_SAME_CITATION]->(p)",
                {batchSize: $batchSize}
            )
            """)
        ]
        
        with self.driver.session() as session:
            for rel_type, relationship_query in relationships:
                session.run(relationship_query, batchSize=Config.RELATIONSHIP_BATCH_SIZE)
                count = self.get_relationship_count(rel_type)
                self.logger.info(f"Created {count} {rel_type} relationships")
def main():
    # Initialize connection
    uri = "neo4j://localhost:7687"
    
    connector = Neo4jConnector(uri)
    
    try:
        # Create constraints
        connector.logger.info("Creating constraints...")
        connector.create_constraints()

        # Load nodes
        connector.logger.info("Loading Citations...")
        connector.load_citations()
        
        connector.logger.info("Loading Sentences...")
        connector.load_sentences()
        
        connector.logger.info("Loading Entities...")
        connector.load_entities()
        
        connector.logger.info("Loading Predications...")
        connector.load_predications()

        # Create relationships
        connector.logger.info("Creating relationships...")
        connector.create_relationships()
        
        # Log final statistics
        connector.logger.info("=== Final Database Statistics ===")
        for label in ["Citation", "Sentence", "Entity", "Predication"]:
            count = connector.get_node_count(label)
            connector.logger.info(f"Total {label} nodes: {count}")
        
        total_rels = connector.get_relationship_count()
        connector.logger.info(f"Total relationships: {total_rels}")

    except Exception as e:
        connector.logger.error(f"An error occurred: {str(e)}")
    finally:
        connector.close()

if __name__ == "__main__":
    main()