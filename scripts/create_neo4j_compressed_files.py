#!/usr/bin/env python3
"""
Neo4j Import Files Compressor

This script creates compressed archives of Neo4j import files specifically for use with Neo4j's import tool.
Neo4j supports .gz compressed files directly during import.
"""

import os
import sys
import gzip
import shutil
import argparse
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Neo4jCompressor:
    def __init__(self, source_dir="neo4j_import_files", output_dir="neo4j_compressed"):
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def get_file_list(self):
        """Get list of all CSV files in source directory"""
        if not self.source_dir.exists():
            raise FileNotFoundError(f"Source directory {self.source_dir} does not exist")
        
        # Only process CSV files for Neo4j import
        files = [f for f in self.source_dir.iterdir() if f.is_file() and f.suffix.lower() == '.csv']
        logger.info(f"Found {len(files)} CSV files to compress")
        return files
    
    def get_total_size(self, files):
        """Calculate total size of files"""
        total_size = sum(f.stat().st_size for f in files)
        return total_size
    
    def format_size(self, size_bytes):
        """Convert bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"
    
    def create_gzip_files(self, compression_level=9):
        """Create individual gzip archives for each CSV file - Neo4j can import these directly"""
        files = self.get_file_list()
        total_original_size = self.get_total_size(files)
        total_compressed_size = 0
        
        logger.info(f"Creating gzip compressed files for Neo4j import (compression level {compression_level})")
        logger.info(f"Original total size: {self.format_size(total_original_size)}")
        
        compressed_files = []
        
        for file_path in files:
            # Keep the .csv extension in the compressed filename for Neo4j compatibility
            gzip_path = self.output_dir / f"{file_path.name}.gz"
            
            logger.info(f"Compressing {file_path.name}...")
            
            with open(file_path, 'rb') as f_in:
                with gzip.open(gzip_path, 'wb', compresslevel=compression_level) as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            compressed_size = gzip_path.stat().st_size
            total_compressed_size += compressed_size
            compressed_files.append(gzip_path)
            
            original_size = file_path.stat().st_size
            compression_ratio = (1 - compressed_size / original_size) * 100
            
            logger.info(f"  {file_path.name}: {self.format_size(original_size)} -> {self.format_size(compressed_size)} ({compression_ratio:.1f}% compression)")
        
        overall_compression = (1 - total_compressed_size / total_original_size) * 100
        logger.info(f"Gzip compression complete!")
        logger.info(f"Total compressed size: {self.format_size(total_compressed_size)}")
        logger.info(f"Overall compression ratio: {overall_compression:.1f}%")
        logger.info(f"Compressed files saved to: {self.output_dir.absolute()}")
        
        return compressed_files, total_compressed_size
    
    def create_import_script(self, compressed_files):
        """Create a Neo4j import script using the compressed files"""
        script_path = self.output_dir / "neo4j_import.sh"
        
        # Get the directory where Neo4j is installed (this will need to be customized)
        neo4j_home = "${NEO4J_HOME:-/usr/local/neo4j}"  # Default path, can be overridden
        
        with open(script_path, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("# Neo4j Import Script for Compressed Files\n")
            f.write("# Generated automatically by create_neo4j_compressed_files.py\n\n")
            
            f.write(f"NEO4J_HOME={neo4j_home}\n")
            f.write("IMPORT_DIR=$(pwd)\n\n")
            
            f.write("# Stop Neo4j before import\n")
            f.write("echo 'Stopping Neo4j...'\n")
            f.write("$NEO4J_HOME/bin/neo4j stop\n\n")
            
            f.write("# Clear existing database\n")
            f.write("echo 'Clearing existing database...'\n")
            f.write("rm -rf $NEO4J_HOME/data/databases/neo4j\n")
            f.write("rm -rf $NEO4J_HOME/data/transactions/neo4j\n\n")
            
            f.write("# Run import with compressed files\n")
            f.write("echo 'Starting Neo4j import...'\n")
            f.write("$NEO4J_HOME/bin/neo4j-admin import \\\n")
            
            # Add node files
            node_files = [f for f in compressed_files if 'nodes_' in f.name]
            for node_file in node_files:
                label = node_file.name.replace('nodes_', '').replace('.csv.gz', '')
                f.write(f"  --nodes={label}={node_file.name} \\\n")
            
            # Add relationship files
            rel_files = [f for f in compressed_files if 'relationships_' in f.name]
            for rel_file in rel_files:
                rel_type = rel_file.name.replace('relationships_', '').replace('.csv.gz', '')
                f.write(f"  --relationships={rel_type}={rel_file.name} \\\n")
            
            f.write("  --database=neo4j \\\n")
            f.write("  --delimiter=, \\\n")
            f.write("  --array-delimiter=; \\\n")
            f.write("  --skip-bad-relationships=true \\\n")
            f.write("  --skip-duplicate-nodes=true \\\n")
            f.write("  --high-io=true\n\n")
            
            f.write("# Start Neo4j\n")
            f.write("echo 'Starting Neo4j...'\n")
            f.write("$NEO4J_HOME/bin/neo4j start\n\n")
            
            f.write("echo 'Import complete! Neo4j is starting up...'\n")
            f.write("echo 'You can access Neo4j Browser at: http://localhost:7474'\n")
        
        # Make the script executable
        os.chmod(script_path, 0o755)
        
        logger.info(f"Created Neo4j import script: {script_path}")
        return script_path
    
    def create_readme(self, compressed_files, total_compressed_size):
        """Create a README file with import instructions"""
        readme_path = self.output_dir / "README.md"
        
        with open(readme_path, 'w') as f:
            f.write("# Neo4j Import Files (Compressed)\n\n")
            f.write("This directory contains compressed CSV files ready for Neo4j import.\n\n")
            
            f.write("## Files\n\n")
            f.write(f"Total compressed size: {self.format_size(total_compressed_size)}\n\n")
            
            f.write("### Node Files\n")
            node_files = [f for f in compressed_files if 'nodes_' in f.name]
            for node_file in sorted(node_files):
                size = self.format_size(node_file.stat().st_size)
                f.write(f"- `{node_file.name}` ({size})\n")
            
            f.write("\n### Relationship Files\n")
            rel_files = [f for f in compressed_files if 'relationships_' in f.name]
            for rel_file in sorted(rel_files):
                size = self.format_size(rel_file.stat().st_size)
                f.write(f"- `{rel_file.name}` ({size})\n")
            
            f.write("\n## Import Instructions\n\n")
            f.write("### Option 1: Use the provided import script\n")
            f.write("```bash\n")
            f.write("cd /path/to/this/directory\n")
            f.write("chmod +x neo4j_import.sh\n")
            f.write("./neo4j_import.sh\n")
            f.write("```\n\n")
            
            f.write("### Option 2: Manual import\n")
            f.write("1. Stop Neo4j: `$NEO4J_HOME/bin/neo4j stop`\n")
            f.write("2. Clear existing database:\n")
            f.write("   ```bash\n")
            f.write("   rm -rf $NEO4J_HOME/data/databases/neo4j\n")
            f.write("   rm -rf $NEO4J_HOME/data/transactions/neo4j\n")
            f.write("   ```\n")
            f.write("3. Run import command:\n")
            f.write("   ```bash\n")
            f.write("   $NEO4J_HOME/bin/neo4j-admin import \\\n")
            
            # Add node files
            for node_file in sorted(node_files):
                label = node_file.name.replace('nodes_', '').replace('.csv.gz', '')
                f.write(f"     --nodes={label}={node_file.name} \\\n")
            
            # Add relationship files
            for rel_file in sorted(rel_files):
                rel_type = rel_file.name.replace('relationships_', '').replace('.csv.gz', '')
                f.write(f"     --relationships={rel_type}={rel_file.name} \\\n")
            
            f.write("     --database=neo4j \\\n")
            f.write("     --delimiter=, \\\n")
            f.write("     --array-delimiter=; \\\n")
            f.write("     --skip-bad-relationships=true \\\n")
            f.write("     --skip-duplicate-nodes=true \\\n")
            f.write("     --high-io=true\n")
            f.write("   ```\n")
            f.write("4. Start Neo4j: `$NEO4J_HOME/bin/neo4j start`\n\n")
            
            f.write("## Notes\n\n")
            f.write("- These files are compressed with gzip and can be imported directly by Neo4j\n")
            f.write("- The import process will automatically decompress the files\n")
            f.write("- Make sure you have enough disk space for the uncompressed data\n")
            f.write("- The import process may take several minutes depending on your system\n")
        
        logger.info(f"Created README file: {readme_path}")

def main():
    parser = argparse.ArgumentParser(description="Create compressed Neo4j import files")
    parser.add_argument("--source", default="neo4j_import_files", help="Source directory (default: neo4j_import_files)")
    parser.add_argument("--output", default="neo4j_compressed", help="Output directory (default: neo4j_compressed)")
    parser.add_argument("--compression-level", type=int, default=9, help="Compression level 1-9 (default: 9)")
    parser.add_argument("--no-script", action="store_true", help="Don't create import script")
    parser.add_argument("--no-readme", action="store_true", help="Don't create README file")
    
    args = parser.parse_args()
    
    try:
        compressor = Neo4jCompressor(args.source, args.output)
        
        # Create compressed files
        compressed_files, total_compressed_size = compressor.create_gzip_files(args.compression_level)
        
        # Create import script
        if not args.no_script:
            compressor.create_import_script(compressed_files)
        
        # Create README
        if not args.no_readme:
            compressor.create_readme(compressed_files, total_compressed_size)
        
        logger.info(f"\nCompression complete! Files saved to: {compressor.output_dir.absolute()}")
        logger.info("You can now use these compressed files with Neo4j's import tool.")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 