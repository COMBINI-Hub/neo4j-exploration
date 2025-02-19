from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv

load_dotenv()

# Initialize the Neo4jGraph
graph = Neo4jGraph(
    url=NEO4J_URI,
    username="",
    password="",
    database="neo4j",
    refresh_schema=True,
    enhanced_schema=True,
)

# Create the GraphCypherQAChain
chain = GraphCypherQAChain.from_llm(
    ChatOpenAI(temperature=0, openai_api_key=OPENAI_API_KEY), graph=graph, verbose=True, allow_dangerous_requests=True
)

# Run the chain with a query
chain.run("what proteins are associated with FOS")