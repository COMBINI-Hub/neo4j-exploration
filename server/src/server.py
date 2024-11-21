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
logger.info(f"Connecting to Neo4j at {uri}")

# Create Neo4j driver
try:
    driver = GraphDatabase.driver(uri)
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
        logger