from neo4j import GraphDatabase
import sys
from colorama import init, Fore, Style

# Initialize colorama for cross-platform color support
init()

def log_status(message):
    print(f"{Fore.GREEN}[✓]{Style.RESET_ALL} {message}")

def log_warning(message):
    print(f"{Fore.YELLOW}[!]{Style.RESET_ALL} {message}")

def log_error(message):
    print(f"{Fore.RED}[✗]{Style.RESET_ALL} {message}")

def test_connection(uri, username, password):
    log_warning(f"Attempting to connect to Neo4j at {uri}")
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        with driver.session() as session:
            log_status("Successfully established connection")
            
            log_warning("Testing query execution...")
            result = session.run("MATCH (n) RETURN count(n) as count")
            count = result.single()["count"]
            log_status(f"Query successful! Found {count} nodes")
            
        driver.close()
        log_status("Connection test completed successfully")
        return True
    except Exception as e:
        log_error(f"Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    uri = sys.argv[1] if len(sys.argv) > 1 else "neo4j://localhost:7687"
    username = sys.argv[2] if len(sys.argv) > 2 else "neo4j"
    password = sys.argv[3] if len(sys.argv) > 3 else "yourpassword"
    
    test_connection(uri, username, password)