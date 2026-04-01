# PrimeKG Neo4j Setup

Simple single-container Neo4j instance with PrimeKG data.

## Quick Start

1. **Start Neo4j** (first time, without data):
   ```bash
   docker-compose up -d
   ```

2. **Import PrimeKG data**:
   ```bash
   ./import_data.sh
   ```

3. **Access Neo4j Browser**:
   - URL: http://localhost:7474
   - Username: `neo4j`
   - Password: `neo4j`

## Data Files

The import script expects the following files in `../primekg_data/`:
- `unique_nodes.csv` - Node data
- `unique_edges.csv` - Relationship data

## Container Management

- **Start**: `docker-compose up -d`
- **Stop**: `docker-compose stop`
- **View logs**: `docker-compose logs -f neo4j`
- **Remove container and volumes**: `docker-compose down -v`

## Notes

- Default credentials: Username `neo4j`, Password `neo4j`
- Data persists in Docker volumes (`neo4j_data` and `neo4j_logs`).
- The import process requires stopping Neo4j, importing, then restarting.

