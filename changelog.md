# Changelog

## 3/12/2025

### Data Import Process Improvement
- Enhanced data import methodology by transitioning from Python scripts to shell scripts and pre-processed CSV files
- This approach significantly improves import speed and reliability when populating Neo4j instances
- Users can now leverage these optimized resources to efficiently import data into their own Neo4j environments

#### Database Dump Exploration
- Explored database dump files as import option but found limitations with Neo4j Community Edition
- Implemented shell script and CSV-based solution as the optimal alternative

## 3/5/2025

### Infrastructure and Data Processing
- Attempted to deploy Neo4j to Radiant Kubernetes cluster
    - This doesn't work since the Kubernetes cluster doesn't allow us to create PVCs for the Neo4j instance. Maybe let's try a different cluster?
- Researched startup Cypher query options using APOC libraries
- Implemented APOC.cypher.runFile for automated database initialization
- Continued development of preprocessing code for SemmedDB data transformation. 

## 2/26/2025

### SEMMEDDB Data Processsing part 2
- Created a new script to process the SemmedDB data using python. 
    - Merging cols in pandas crashed my kernel
    - Merging cols in dask cause many col data type issues
    - Was stuck here for a while. 
- Tried loading PrimeKG on a radiant cluster but it was too slow. Tried again locally using APOC but I ran out of storage space. But we got the Kubernetes cluster allocation so maybe we can try that?

## 2/19/2025

### Knowledge Graph Integration
- Established approach for SemmedDB ingestion focusing on medical concepts and relations
- Developed strategy for entity loading from prediction table (OBJECT_CUI/SUBJECT_CUI)
- Created plan for joining predication and predication_aux tables to build comprehensive instances
- Applied this method to PrimeKG to create unique_nodes and unique_relationships. 

## 2/12/2025

### SemmedDB Data Model Implementation
- Refined data model for SemmedDB with nodes representing medical concepts
- Established edges to represent medical relations between concepts
- Created additional node type for PMID to link to source sentences
- Developed approach for loading concepts from prediction table
- Designed edge properties to incorporate predicate and predicate_aux data

## 2/5/2025

### Data Processing and Infrastructure
- Refined ingest script for predication auxiliary nodes and relationships
- Acquired PrimeKG dataset and began development of ingest script
- Researched optimization techniques for predication auxiliary loading methods. CREATE was much faster than MERGE so tried to optimize for that.
- Made progress on PrimeKG integration with Neo4j

## 1/21/2025

### SemmedDB Loading Progress
- Successfully loaded majority of SemmedDB relationships into Neo4j (with questionable usability and validity). 
- Implemented chunking strategy to handle large dataset volumes using APOC
- Debugged parsing errors in the ingestion pipeline

## 1/17/2025

### Data Ingestion Optimization
- Completed full dataset ingestion for SemmedDB nodes.
- Implemented optimizations to handle large-scale data loading

## 1/10/2025

### Debugging and Optimization
- Debugged parsing errors in the SemmedDB ingestion process
- Improved error handling for dataframes without headers
- Implemented more robust recovery mechanisms

## 12/11/2024

### Initial Setup
- Successfully downloaded SemmedDB
- Began initial loading process into Neo4j
- Set up Docker Compose configuration for Neo4j environment
    - Resolved issues with network configs in the docker-compose.
    - This ended up being the best method for local dev since the docker-compose file was able to be used to spin up the Neo4j instance and the Python driver was able to be used to load the data.

## 11/20/2024

### Infrastructure Development
- Created Docker container for demo project (client and server)
- Began collaboration with team to determine optimal approach for loading SemmedDB

## 11/12/2024

### Project Organization
- Set up GitHub project for COMBINI knowledge graph work
- Created repository structure for Neo4jExploration

## 11/6/2024

### Initial Research and Setup
- Summarized research notes on knowledge graph implementation
- Learned about Helm charts for Kubernetes deployment
- Dockerized Neo4j instance
- Added scripts for deployment, cleanup, and debugging (which were mostly unessecary since I could just use docker compose)

## 10/18/2024

### Proof of Concept
Created initial proof of concept for knowledge graph implementation using the [HALD](https://figshare.com/articles/dataset/HALD_a_human_aging_and_longevity_knowledge_graph_for_precision_gerontology_and_geroscience_analyses/22828196) dataset. This was using the Neo4j Python driver. The script wouldn't return any statistics and you were unable to view the graph since when the python kernel was terminated, so was the port on which the Neo4j GUI was running.