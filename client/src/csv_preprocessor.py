import pandas as pd
import logging
from typing import Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class CSVPreprocessor:
    """Handles CSV preprocessing using pandas for Neo4j Cypher compatibility"""
    
    def __init__(self):
        self.dtypes = {
            'pmid': 'Int64',  # Handles missing values better than int64
            'sentence_id': 'Int64',
            'entity_id': 'Int64',
            'score': 'float64',
            'pub_year': 'Int64'
        }

    def preprocess_csv(self, input_path: str, chunk_size: Optional[int] = None) -> str:
        """
        Preprocess CSV file using pandas for Neo4j compatibility.
        """
        try:
            logger.info(f"Starting preprocessing of {input_path}")
            
            # Read CSV with appropriate data types
            df = pd.read_csv(
                input_path,
                dtype=self.dtypes,
                na_values=['', 'NULL', 'null', 'NA', 'NaN'],
                keep_default_na=True,
                on_bad_lines='warn',
                encoding='utf-8',
                engine='c'  # Fastest engine
            )
            
            # Clean column names
            df.columns = (df.columns
                         .str.strip()
                         .str.replace('[^0-9a-zA-Z_]', '_', regex=True)
                         .str.lower())
            
            # Fill NA/null values appropriately
            for col in df.select_dtypes(include=['Int64']):
                df[col] = df[col].fillna(0)
            for col in df.select_dtypes(include=['float64']):
                df[col] = df[col].fillna(0.0)
            for col in df.select_dtypes(include=['object']):
                df[col] = df[col].fillna('')
                
            # Clean string columns
            str_cols = df.select_dtypes(include=['object']).columns
            for col in str_cols:
                df[col] = (df[col]
                          .astype(str)
                          .str.strip()
                          .str.replace(r'[\r\n]+', ' ', regex=True)  # Remove newlines
                          .str.replace(r'\s+', ' ', regex=True)      # Normalize spaces
                          .str.replace(r'\\', r'\\\\', regex=True)   # Escape backslashes
                          .str.replace(r'"', r'\"', regex=True))     # Escape quotes
            
            # Write processed CSV
            df.to_csv(
                input_path,
                index=False,
                quoting=1,  # QUOTE_MINIMAL
                escapechar='\\',
                doublequote=False,
                encoding='utf-8'
            )
            
            logger.info(f"Successfully preprocessed {len(df):,} rows in {input_path}")
            return input_path

        except Exception as e:
            logger.error(f"Error preprocessing {input_path}: {str(e)}")
            raise