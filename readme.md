# Neo4j Exploration with SemMedDB

## Overview
This project utilizes data from the Semantic MEDLINE Database (SemMedDB), a repository of semantic predications (subject-predicate-object triples) extracted from titles and abstracts of biomedical literature in PubMed.

## Data Source
The data is sourced from the National Library of Medicine's (NLM) Semantic Knowledge Representation (SKR) project:
- Source: [SemRep/SemMedDB](https://lhncbc.nlm.nih.gov/ii/tools/SemRep_SemMedDB_SKR/dbinfo.html)
- Database: SemMedDB
- Version: Latest public release

## Features
- Processing of biomedical semantic predications
- Analysis of subject-predicate-object relationships
- Integration with PubMed literature references
- Semantic relationship extraction and analysis

## Database Schema
The database includes several key tables:
- PREDICATION: Contains semantic predications
- SENTENCE: Source sentences from which predications were extracted
- CITATIONS: Bibliographic information
- PREDICATION_AUX: Additional predication information

## Prerequisites
- Python 3.x
- CSV Database files from SemMedDB. The titles used were as follows:
    - entity.csv
    - citations.csv
    - predication.csv
    - predication_aux.csv
    - sentence.csv
- Required Python packages (list in requirements.txt)

## Installation
1. Clone the repository:
```bash
git clone https://github.com/drshika/neo4j-exploration.git
```
2. Install the required Python packages:
```bash
pip install -r requirements.txt
```
4. Run the docker-compose file:
```bash
docker-compose up -d
```  
5. View the database at http://localhost:7474/browser/. 
## Usage
1. Set up database configuration
2. Run data import scripts
3. Execute analysis tools

## Data Processing
The project processes biomedical literature data through:
1. Data extraction from SemMedDB
2. Semantic relationship analysis
3. Result aggregation and visualization

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push to the branch
5. Create a Pull Request

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
- National Library of Medicine (NLM)
- Semantic Knowledge Representation project team
- SemRep and SemMedDB developers

## Contact
For questions and support, please open an issue in the repository.

## References
- [SemMedDB Documentation](https://lhncbc.nlm.nih.gov/ii/tools/SemRep_SemMedDB_SKR/dbinfo.html)
- [NLM Semantic Knowledge Resources](https://www.nlm.nih.gov/)