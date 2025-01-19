# SemMedDB Neo4j Data Pipeline

## Overview
A data pipeline for loading and analyzing SemMedDB (Semantic MEDLINE Database) data into Neo4j. This project processes biomedical semantic predications from PubMed literature and creates a graph database for analysis.

## Features
- Automated data loading from CSV/SQL sources
- Configurable batch processing
- Relationship creation between entities
- Memory-optimized for large datasets
- Docker containerization
- Progress logging and error handling

## Prerequisites
- Docker and Docker Compose
- Python 3.x
- Neo4j APOC plugin

## Data Source
The data is sourced from the National Library of Medicine's (NLM) Semantic Knowledge Representation (SKR) project:
- Source: [SemRep/SemMedDB](https://lhncbc.nlm.nih.gov/ii/tools/SemRep_SemMedDB_SKR/dbinfo.html)
- Database: SemMedDB
- Version: Latest public release

## Data Requirements
Place your data files in the appropriate directories:
```bash
    data/
    ├── entity.gz
    ├── citations.csv
    ├── predication.csv
    ├── predication_aux.csv
    └── sentence.csv
```

## Installation
1. Clone the repository:
```bash
git clone https://github.com/drshika/neo4j-exploration.git
```
2. Install the required Python packages:
```bash
pip install -r requirements.txt
```
3. Configure environment:
Edit the configuration in `Config` class (referenced in client.py, lines 6-22) to match your setup.
4. Run the docker-compose file:
```bash
docker-compose up -d
```  
5. View the database at http://localhost:7474/browser/. 

## Data Model
The graph database consists of the following main nodes:
- Citations
- Sentences
- Entities
- Predications

With relationships:
- HAS_ENTITY
- HAS_PREDICATION
- BELONGS_TO_SAME_CITATION

## Acknowledgments
- National Library of Medicine (NLM)
- Semantic Knowledge Representation project team
- SemRep and SemMedDB developers

## Contact
For questions and support, please open an issue in the repository.

## References
- [SemMedDB Documentation](https://lhncbc.nlm.nih.gov/ii/tools/SemRep_SemMedDB_SKR/dbinfo.html)
- [NLM Semantic Knowledge Resources](https://www.nlm.nih.gov/)