import os
import time
import requests
from colorama import init, Fore, Style
from neo4j import GraphDatabase

# Initialize colorama
init()

class KnowledgeGraphClient:
    def __init__(self):
        self.neo4j_uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
        self.neo4j_username = os.getenv("NEO4J_USERNAME", "neo4j")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD", "yourpassword")
        self.server_url = os.getenv("SERVER_URL", "http://kg-server:8080")

    def log_status(self, message):
        print(f"{Fore.GREEN}[✓]{Style.RESET_ALL} {message}")

    def log_warning(self, message):
        print(f"{Fore.YELLOW}[!]{Style.RESET_ALL} {message}")

    def log_error(self, message):
        print(f"{Fore.RED}[✗]{Style.RESET_ALL} {message}")

    def test_connection(self):
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Test Neo4j connection using system database
                driver = GraphDatabase.driver(
                    self.neo4j_uri,
                    auth=(self.neo4j_username, self.neo4j_password)
                )
                with driver.session(database="system") as session:
                    # Query database status from system database
                    result = session.run("SHOW DATABASE neo4j")
                    record = result.single()
                    if record and record["currentStatus"] == "online":
                        self.log_status("Successfully connected to Neo4j")
                    else:
                        raise Exception("Database is not online")
                driver.close()
                
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    self.log_warning(f"Connection attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.log_error(f"Connection failed after {max_retries} attempts: {str(e)}")
                    return False

if __name__ == "__main__":
    client = KnowledgeGraphClient()
    client.test_connection()