from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
import os
import uvicorn
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Neo4j connection configuration
uri = os.getenv("NEO4J_URI", "neo4j://localhost:7687")
username = os.getenv("NEO4J_USERNAME", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "yourpassword")

logger.info(f"Connecting to Neo4j at {uri} with username: {username}")

# Create Neo4j driver
try:
    driver = GraphDatabase.driver(uri, auth=(username, password))
    # Verify connection
    with driver.session() as session:
        result = session.run("RETURN 1")
        result.single()
    logger.info("Successfully connected to Neo4j")
except Exception as e:
    logger.error(f"Failed to connect to Neo4j: {str(e)}")
    raise

@app.get("/health")
async def health_check():
    try:
        with driver.session() as session:
            result = session.run("RETURN 1")
            result.single()
        logger.info("Health check passed")
        return {"status": "healthy"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    logger.info("Starting FastAPI server on port 8080")
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")