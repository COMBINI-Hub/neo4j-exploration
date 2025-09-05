#!/usr/bin/env python3
"""
KG-Bioportal Complete Download and Merge Script

This script downloads all 1050 ontologies from KG-Bioportal and merges them into
a single knowledge graph in KGX format for Neo4j import.

Data source: https://kghub.io/kg-bioportal/
Reference: https://ncbo.github.io/kg-bioportal/

Author: Neo4j Exploration Team
"""

import pandas as pd
import requests
import tarfile
import tempfile
import logging
import time
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from urllib.parse import urljoin
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KGBioportalDownloader:
    """
    Downloads and merges all KG-Bioportal ontologies into a single knowledge graph
    """
    
    def __init__(self, output_dir: str = "."):
        self.base_url = "https://kghub.io/kg-bioportal"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Statistics tracking
        self.stats = {
            'total_ontologies': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'total_nodes': 0,
            'total_edges': 0,
            'failed_ontologies': []
        }
    
    def get_all_ontologies(self) -> List[str]:
        """
        Get list of all available ontologies from KG-Bioportal
        
        Returns:
            List of ontology acronyms
        """
        logger.info("Fetching list of all ontologies...")
        
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            
            # Extract ontology names from the directory listing
            # Look for patterns like: href="https://kghub.io/kg-bioportal/ONTOLOGY_NAME"
            ontology_pattern = r'href="https://kghub\.io/kg-bioportal/([^/"]+)"'
            ontologies = re.findall(ontology_pattern, response.text)
            
            # Filter out non-ontology entries and clean up
            ontologies = [onto for onto in ontologies if onto and not onto.startswith('http')]
            
            logger.info(f"Found {len(ontologies)} ontologies")
            return ontologies
            
        except Exception as e:
            logger.error(f"Failed to fetch ontology list: {e}")
            return []
    
    def get_ontology_versions(self, ontology: str) -> List[str]:
        """
        Get available versions for an ontology
        
        Args:
            ontology: Ontology acronym
            
        Returns:
            List of version numbers (latest first)
        """
        try:
            url = f"{self.base_url}/{ontology}/"
            response = requests.get(url)
            response.raise_for_status()
            
            # Parse HTML to find version directories
            version_pattern = rf'href="https://kghub\.io/kg-bioportal/{ontology}/(\d+)/index\.html"'
            versions = re.findall(version_pattern, response.text)
            
            # Sort versions numerically (latest first)
            versions = sorted(versions, key=int, reverse=True)
            
            return versions
            
        except Exception as e:
            logger.warning(f"Failed to get versions for {ontology}: {e}")
            return []
    
    def download_ontology_data(self, ontology: str, version: str = None) -> Optional[Dict]:
        """
        Download ontology data from KG-Bioportal
        
        Args:
            ontology: Ontology acronym
            version: Specific version (if None, uses latest)
            
        Returns:
            Dictionary with nodes and edges DataFrames, or None if failed
        """
        try:
            # Get latest version if not specified
            if version is None:
                versions = self.get_ontology_versions(ontology)
                if not versions:
                    logger.warning(f"No versions found for {ontology}")
                    return None
                version = versions[0]
            
            logger.info(f"Downloading {ontology} version {version}")
            
            # Download the tar.gz file
            tar_url = f"{self.base_url}/{ontology}/{version}/{ontology}.tar.gz"
            response = requests.get(tar_url, stream=True)
            response.raise_for_status()
            
            # Extract the tar.gz file
            with tempfile.NamedTemporaryFile(suffix='.tar.gz') as temp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
                temp_file.flush()
                
                # Extract files
                with tarfile.open(temp_file.name, 'r:gz') as tar:
                    tar.extractall(tempfile.gettempdir())
                
                # Read nodes and edges files
                nodes_file = Path(tempfile.gettempdir()) / f"{ontology}_nodes.tsv"
                edges_file = Path(tempfile.gettempdir()) / f"{ontology}_edges.tsv"
                
                if not nodes_file.exists() or not edges_file.exists():
                    logger.warning(f"Missing files for {ontology}")
                    return None
                
                # Read TSV files with proper handling of line breaks in fields
                try:
                    nodes_df = pd.read_csv(nodes_file, sep='\t', low_memory=False, quoting=3, on_bad_lines='skip')
                    edges_df = pd.read_csv(edges_file, sep='\t', low_memory=False, quoting=3, on_bad_lines='skip')
                except Exception as e:
                    logger.warning(f"Failed to read TSV files for {ontology}: {e}")
                    return None
                
                # Add ontology information
                nodes_df['source_ontology'] = ontology
                edges_df['source_ontology'] = ontology
                
                logger.info(f"Downloaded {ontology}: {len(nodes_df)} nodes, {len(edges_df)} edges")
                
                return {
                    'ontology': ontology,
                    'version': version,
                    'nodes': nodes_df,
                    'edges': edges_df
                }
                
        except Exception as e:
            logger.warning(f"Failed to download {ontology}: {e}")
            return None
    
    def download_all_ontologies(self, max_ontologies: int = None, delay: float = 1.0) -> Dict[str, Dict]:
        """
        Download data for all available ontologies
        
        Args:
            max_ontologies: Maximum number of ontologies to download (None for all)
            delay: Delay between downloads in seconds
            
        Returns:
            Dictionary mapping ontology names to their data
        """
        logger.info("Starting download of all KG-Bioportal ontologies...")
        
        # Get all ontologies
        all_ontologies = self.get_all_ontologies()
        
        if max_ontologies:
            all_ontologies = all_ontologies[:max_ontologies]
        
        self.stats['total_ontologies'] = len(all_ontologies)
        downloaded_data = {}
        
        for i, ontology in enumerate(all_ontologies, 1):
            logger.info(f"Processing {ontology} ({i}/{len(all_ontologies)})...")
            
            data = self.download_ontology_data(ontology)
            
            if data:
                downloaded_data[ontology] = data
                self.stats['successful_downloads'] += 1
                self.stats['total_nodes'] += len(data['nodes'])
                self.stats['total_edges'] += len(data['edges'])
                logger.info(f"Successfully downloaded {ontology}")
            else:
                self.stats['failed_downloads'] += 1
                self.stats['failed_ontologies'].append(ontology)
                logger.warning(f"Failed to download {ontology}")
            
            # Be respectful to the server
            if delay > 0:
                time.sleep(delay)
        
        logger.info(f"Download complete: {self.stats['successful_downloads']} successful, {self.stats['failed_downloads']} failed")
        return downloaded_data
    
    def merge_nodes(self, downloaded_data: Dict[str, Dict]) -> pd.DataFrame:
        """
        Merge all node data into a single DataFrame
        
        Args:
            downloaded_data: Dictionary of downloaded ontology data
            
        Returns:
            Merged nodes DataFrame
        """
        logger.info("Merging all nodes...")
        
        all_nodes = []
        
        for ontology, data in downloaded_data.items():
            nodes_df = data['nodes'].copy()
            
            # Ensure we have required columns
            if 'id' not in nodes_df.columns and 'name' in nodes_df.columns:
                # Create id from name if missing
                nodes_df['id'] = nodes_df['name']
            
            all_nodes.append(nodes_df)
        
        if all_nodes:
            merged_nodes = pd.concat(all_nodes, ignore_index=True)
            
            # Remove duplicates based on id
            if 'id' in merged_nodes.columns:
                initial_count = len(merged_nodes)
                merged_nodes = merged_nodes.drop_duplicates(subset=['id'], keep='first')
                final_count = len(merged_nodes)
                logger.info(f"Merged nodes: {initial_count} -> {final_count} (removed {initial_count - final_count} duplicates)")
            
            return merged_nodes
        else:
            logger.warning("No nodes to merge")
            return pd.DataFrame()
    
    def merge_edges(self, downloaded_data: Dict[str, Dict]) -> pd.DataFrame:
        """
        Merge all edge data into a single DataFrame
        
        Args:
            downloaded_data: Dictionary of downloaded ontology data
            
        Returns:
            Merged edges DataFrame
        """
        logger.info("Merging all edges...")
        
        all_edges = []
        
        for ontology, data in downloaded_data.items():
            edges_df = data['edges'].copy()
            all_edges.append(edges_df)
        
        if all_edges:
            merged_edges = pd.concat(all_edges, ignore_index=True)
            logger.info(f"Merged {len(merged_edges)} total edges")
            return merged_edges
        else:
            logger.warning("No edges to merge")
            return pd.DataFrame()
    
    def sanitize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize all string fields in the DataFrame by removing newlines and tabs
        """
        for col in df.columns:
            if df[col].dtype == object:
                # Replace NaN with empty string first
                df[col] = df[col].fillna('')
                # Then replace newlines and tabs
                df[col] = df[col].astype(str).str.replace(r'[\r\n\t]', ' ', regex=True)
                # Remove any remaining 'nan' strings
                df[col] = df[col].replace('nan', '')
        return df
    
    def save_merged_data(self, nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> Dict[str, str]:
        """
        Save merged data as KGX format files, sanitized for Neo4j import
        """
        logger.info("Saving merged data...")
        created_files = {}
        # Sanitize dataframes
        nodes_df = self.sanitize_dataframe(nodes_df)
        edges_df = self.sanitize_dataframe(edges_df)
        # Save merged nodes
        if len(nodes_df) > 0:
            nodes_file = self.output_dir / "merged_nodes.tsv"
            nodes_df.to_csv(nodes_file, sep='\t', index=False)
            created_files['nodes'] = str(nodes_file)
            logger.info(f"Saved {len(nodes_df)} nodes to {nodes_file}")
        # Save merged edges
        if len(edges_df) > 0:
            edges_file = self.output_dir / "merged_edges.tsv"
            edges_df.to_csv(edges_file, sep='\t', index=False)
            created_files['edges'] = str(edges_file)
            logger.info(f"Saved {len(edges_df)} edges to {edges_file}")
        return created_files
    
    def print_statistics(self):
        """Print download and merge statistics"""
        print("\n" + "="*60)
        print("KG-BIOPORTAL DOWNLOAD STATISTICS")
        print("="*60)
        print(f"Total ontologies processed: {self.stats['total_ontologies']}")
        print(f"Successful downloads: {self.stats['successful_downloads']}")
        print(f"Failed downloads: {self.stats['failed_downloads']}")
        print(f"Success rate: {self.stats['successful_downloads']/self.stats['total_ontologies']*100:.1f}%")
        print(f"Total nodes: {self.stats['total_nodes']:,}")
        print(f"Total edges: {self.stats['total_edges']:,}")
        
        if self.stats['failed_ontologies']:
            print(f"\nFailed ontologies ({len(self.stats['failed_ontologies'])}):")
            for onto in self.stats['failed_ontologies'][:10]:  # Show first 10
                print(f"  - {onto}")
            if len(self.stats['failed_ontologies']) > 10:
                print(f"  ... and {len(self.stats['failed_ontologies']) - 10} more")
    
    def process_all_ontologies(self, max_ontologies: int = None, delay: float = 1.0) -> Dict:
        """
        Main processing pipeline for all KG-Bioportal ontologies
        
        Args:
            max_ontologies: Maximum number of ontologies to download
            delay: Delay between downloads in seconds
            
        Returns:
            Processing results dictionary
        """
        logger.info("Starting complete KG-Bioportal processing...")
        
        try:
            # Download all ontology data
            downloaded_data = self.download_all_ontologies(max_ontologies, delay)
            
            if not downloaded_data:
                return {
                    'success': False,
                    'error': 'No data downloaded'
                }
            
            # Merge nodes and edges
            nodes_df = self.merge_nodes(downloaded_data)
            edges_df = self.merge_edges(downloaded_data)
            
            # Save merged data
            created_files = self.save_merged_data(nodes_df, edges_df)
            
            # Print statistics
            self.print_statistics()
            
            return {
                'success': True,
                'total_entities': len(nodes_df),
                'total_relationships': len(edges_df),
                'files': created_files,
                'statistics': self.stats
            }
            
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download and merge all KG-Bioportal ontologies')
    parser.add_argument('--max-ontologies', type=int, default=None, 
                       help='Maximum ontologies to download (default: all 1050)')
    parser.add_argument('--output-dir', default='.', 
                       help='Output directory for merged files')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between downloads in seconds (default: 1.0)')
    
    args = parser.parse_args()
    
    downloader = KGBioportalDownloader(output_dir=args.output_dir)
    
    # Process all ontologies
    results = downloader.process_all_ontologies(
        max_ontologies=args.max_ontologies,
        delay=args.delay
    )
    
    if results['success']:
        print("\n" + "="*60)
        print("KG-BIOPORTAL PROCESSING COMPLETE")
        print("="*60)
        print(f"Total entities: {results['total_entities']:,}")
        print(f"Total relationships: {results['total_relationships']:,}")
        print(f"Output files: {list(results['files'].keys())}")
        
        print(f"\nFiles created:")
        for file_type, file_path in results['files'].items():
            print(f"  {file_type}: {file_path}")
        
        print(f"\nNext steps:")
        print(f"1. Import into Neo4j using the merged TSV files")
        print(f"2. Files are in KGX format and ready for graph database import")
        
    else:
        print(f"Processing failed: {results.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main() 