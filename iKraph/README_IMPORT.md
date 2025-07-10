# iKraph Neo4j Import (Compressed Files)

This directory contains everything needed to import the iKraph knowledge graph into Neo4j using compressed files.

## What's Included

- `docker_import.sh` - The main import script
- `docker-compose.yml` - Docker configuration for Neo4j
- `import/` - Directory containing compressed CSV files (`.csv.gz`)

## Quick Start

1. **Prerequisites**: Make sure you have Docker and Docker Compose installed

2. **Run the import**:
   ```bash
   chmod +x docker_import.sh
   ./docker_import.sh
   ```

3. **Access Neo4j Browser**: Open http://localhost:7474 in your web browser

## What the Script Does

1. Checks that all required compressed files are present in the `import/` directory
2. Starts a Neo4j Docker container with the proper configuration
3. Verifies the compressed files are accessible in the container
4. Runs the Neo4j admin import with all the compressed `.csv.gz` files
5. Starts Neo4j and verifies the import was successful

## File Structure

```
iKraph/
├── docker_import.sh          # Main import script
├── docker-compose.yml        # Docker configuration
├── import/                   # Compressed files directory
│   ├── nodes_*.csv.gz        # Node files (12 files)
│   └── relationships_*.csv.gz # Relationship files (2 files)
└── README_IMPORT.md          # This file
```

## Compressed Files

The import uses 14 compressed files:
- **12 node files**: anatomy, biological process, cellline, chemical, cellular component, disease, dnamutation, gene, molecular function, pathway, pharmacologic class, species
- **2 relationship files**: db_relationship, pubmed_relationship

Total compressed size: ~888 MB (vs 6.13 GB uncompressed)

## Troubleshooting

- **"Import directory not found"**: Make sure the `import/` directory exists and contains the `.csv.gz` files
- **"Compressed files not found"**: Ensure all required compressed files are in the `import/` directory
- **Docker issues**: Make sure Docker and Docker Compose are running

## Verification

After import, you can verify the data by running these Cypher queries in Neo4j Browser:

```cypher
// Count nodes by type
MATCH (n) RETURN labels(n) as type, count(*) as count;

// Count relationships by type  
MATCH ()-[r]->() RETURN type(r) as type, count(*) as count;
```

## Stopping the Container

```bash
cd iKraph
docker-compose down
``` 