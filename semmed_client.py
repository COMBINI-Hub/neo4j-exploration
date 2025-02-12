from neo4j import GraphDatabase
import logging
import time
import argparse  

def parse_args():
    parser = argparse.ArgumentParser(description='Load data into Neo4j database')
    parser.add_argument('--constraints', action='store_true', help='Create constraints and indexes')
    parser.add_argument('--citations', action='store_true', help='Load citations')
    parser.add_argument('--sentences', action='store_true', help='Load sentences')
    parser.add_argument('--entities', action='store_true', help='Load entities')
    parser.add_argument('--predications', action='store_true', help='Load predications')
    parser.add_argument('--relationships', action='store_true', help='Create relationships')
    parser.add_argument('--all', action='store_true', help='Load everything')
    return parser.parse_args()

# Configuration
class Config:
    # Database connection
    NEO4J_URI = "neo4j://localhost:7687"
    
    # Data file paths
    DATA_DIR = "data"
    CITATIONS_FILE = f"{DATA_DIR}/citations.csv"
    SENTENCES_FILE = f"{DATA_DIR}/sentences.csv"
    ENTITIES_FILE = f"{DATA_DIR}/entity.gz"
    PREDICATIONS_FILE = f"{DATA_DIR}/predication.csv"
    PREDICATION_AUX_FILE = f"{DATA_DIR}/predication_aux.csv"
        
    # Batch sizes for different operations
    CITATION_BATCH_SIZE = 1000
    SENTENCE_BATCH_SIZE = 1000
    ENTITY_BATCH_SIZE = 1000
    PREDICATION_BATCH_SIZE = 1000
    RELATIONSHIP_BATCH_SIZE = 500
class Neo4jConnector:
    def __init__(self, uri):
        self.driver = GraphDatabase.driver(uri)
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
    
    # WIP
    def load_predications(self):
        # Step 1: Create predication nodes from PREDICATION table
        create_predications_query = """
    CALL apoc.periodic.iterate(
        'CALL apoc.load.csv($file, {separator:",", quoteChar:"\\"", nullValues:["\\\\N"]}) YIELD list 
         RETURN 
                trim(coalesce(list[0], "")) as predication_id,
                trim(coalesce(list[1], "")) as sentence_id,
                trim(coalesce(list[2], "")) as pmid,
                trim(coalesce(list[3], "")) as predicate,
                trim(coalesce(list[4], "")) as subject_cui,
                trim(coalesce(list[5], "")) as subject_name,
                trim(coalesce(list[6], "")) as subject_semtype,
                trim(coalesce(list[7], "")) as subject_novelty,
                trim(coalesce(list[8], "")) as object_cui,
                trim(coalesce(list[9], "")) as object_name,
                trim(coalesce(list[10], "")) as object_semtype,
                trim(coalesce(list[11], "")) as object_novelty',
        'CREATE (p:Predication {
            predication_id: predication_id,
            sentence_id: sentence_id,
            pmid: pmid,
            predicate: predicate,
            subject_cui: subject_cui,
            subject_name: subject_name,
            subject_semtype: subject_semtype,
            subject_novelty: subject_novelty,
            object_cui: object_cui,
            object_name: object_name,
            object_semtype: object_semtype,
            object_novelty: object_novelty
        })',
        {
            batchSize: $batchSize, 
            iterateList: true, 
            parallel: false,
            params: {file: $file}
        }
    )
    """
        
        # Step 2: Update predications with auxiliary information
        update_predications_query = """
        CALL apoc.periodic.iterate(
            'CALL apoc.load.csv($file, {separator:",", header:false}) YIELD list 
            RETURN 
                    trim(list[0]) as aux_id,
                    trim(list[1]) as predication_id,
                    trim(list[2]) as subject_text,
                    trim(list[3]) as subject_dist,
                    trim(list[4]) as subject_maxdist,
                    trim(list[5]) as subject_start_index,
                    trim(list[6]) as subject_end_index,
                    trim(list[7]) as subject_score,
                    trim(list[8]) as indicator_type,
                    trim(list[9]) as predicate_start_index,
                    trim(list[10]) as predicate_end_index,
                    trim(list[11]) as object_text,
                    trim(list[12]) as object_dist,
                    trim(list[13]) as object_maxdist,
                    trim(list[14]) as object_start_index,
                    trim(list[15]) as object_end_index,
                    trim(list[16]) as object_score',
            'MATCH (p:Predication {predication_id: predication_id})
            SET p += {
                aux_id: aux_id,
                subject_text: subject_text,
                subject_dist: subject_dist,
                subject_maxdist: subject_maxdist,
                subject_start_index: subject_start_index,
                subject_end_index: subject_end_index,
                subject_score: subject_score,
                indicator_type: indicator_type,
                predicate_start_index: predicate_start_index,
                predicate_end_index: predicate_end_index,
                object_text: object_text,
                object_dist: object_dist,
                object_maxdist: object_maxdist,
                object_start_index: object_start_index,
                object_end_index: object_end_index,
                object_score: object_score
            }',
            {
                batchSize: $batchSize, 
                iterateList: true, 
                parallel: false, 
                params: {file: $file}
            }
        )
        """
        
        with self.driver.session() as session:
            # Step 1: Create predication nodes
            self.logger.info("Creating predication nodes...")
            session.run(create_predications_query, 
                    file=Config.PREDICATIONS_FILE, 
                    batchSize=Config.PREDICATION_BATCH_SIZE)
            count = self.get_node_count("Predication")
            self.logger.info(f"Created {count} predication nodes")
            
            # Step 2: Update with auxiliary information
            self.logger.info("Updating predications with auxiliary information...")
            session.run(update_predications_query, 
                    file=Config.PREDICATION_AUX_FILE, 
                    batchSize=Config.PREDICATION_BATCH_SIZE)
            self.logger.info("Finished updating predications")
            
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
                   trim(list[1]) as pmid,
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
                pmid: pmid,
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
    args = parse_args()
    
    try:
        run_all = args.all or not any([args.constraints, args.citations, args.sentences, 
                                     args.entities, args.predications, args.relationships])

        if run_all or args.constraints:
            connector.logger.info("Creating constraints...")
            connector.create_constraints()

        if run_all or args.citations:
            connector.logger.info("Loading Citations...")
            connector.load_citations()
        
        if run_all or args.sentences:
            connector.logger.info("Loading Sentences...")
            connector.load_sentences()
        
        if run_all or args.entities:
            connector.logger.info("Loading Entities...")
            connector.load_entities()
        
        if run_all or args.predications:
            connector.logger.info("Loading Predications...")
            connector.load_predications()

        if run_all or args.relationships:
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