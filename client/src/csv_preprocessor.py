import csv
import os
import shutil
from tqdm import tqdm
import logging
import re
from typing import List, Any

# Configure logger
logger = logging.getLogger(__name__)

class CSVPreprocessor:
    """Handles CSV preprocessing for Neo4j Cypher compatibility"""
    
    def __init__(self):
        # Compile regex patterns once for efficiency
        self.invalid_chars = re.compile(r'[^\x20-\x7E]')  # Non-printable chars
        self.quote_pattern = re.compile(r'(?<!\\)"')  # Unescaped quotes
        
    @staticmethod
    def count_lines(filepath: str) -> int:
        """Efficiently count lines in a file."""
        with open(filepath, 'rb') as f:
            return sum(1 for _ in f)

    def sanitize_identifier(self, value: str) -> str:
        """
        Sanitize values to conform to Neo4j naming conventions.
        Based on: https://neo4j.com/docs/cypher-manual/current/syntax/naming/
        """
        if not value:
            return ''
            
        # Remove non-printable characters
        value = self.invalid_chars.sub('', value)
        
        # Replace problematic characters with underscores
        value = re.sub(r'[^a-zA-Z0-9_]', '_', value)
        
        # Ensure it starts with a letter (prepend 'n' if needed)
        if value and not value[0].isalpha():
            value = 'n' + value
            
        return value[:16383]  # Neo4j identifier length limit

    def clean_field(self, field: Any) -> str:
        """
        Clean individual field values for Neo4j compatibility.
        Handles various data types and ensures proper escaping.
        """
        if field is None:
            return ''
            
        # Convert to string and strip whitespace
        value = str(field).strip()
        
        if not value:
            return ''
            
        try:
            # Handle numeric values
            if value.replace('.', '').replace('-', '').isdigit():
                if '.' in value:
                    return str(float(value))
                return str(int(float(value)))
                
            # Clean string values
            value = (value
                    .replace('\\', '\\\\')  # Escape backslashes
                    .replace('"', '\\"'))    # Escape quotes
            
            # Remove any remaining problematic characters
            value = self.invalid_chars.sub('', value)
            
            return value
            
        except Exception as e:
            logger.warning(f"Error cleaning field '{field}': {str(e)}")
            return str(field)

    def process_row(self, row: List[str], expected_columns: int = 9) -> List[str]:
        """
        Process a single row, ensuring proper formatting and column count.
        """
        try:
            # Clean each field
            cleaned = [self.clean_field(field) for field in row]
            
            # Pad or truncate to expected number of columns
            if len(cleaned) < expected_columns:
                cleaned.extend([''] * (expected_columns - len(cleaned)))
            elif len(cleaned) > expected_columns:
                cleaned = cleaned[:expected_columns]
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Error processing row: {row}")
            logger.error(f"Error details: {str(e)}")
            return [''] * expected_columns

    def preprocess_csv(self, input_path: str, chunk_size: int = 10000) -> str:
        """
        Preprocess CSV file for Neo4j import compatibility.
        """
        temp_path = input_path + '.tmp'
        
        try:
            # Check available disk space
            required_space = os.path.getsize(input_path) * 2
            available_space = shutil.disk_usage(os.path.dirname(input_path)).free
            if available_space < required_space:
                raise RuntimeError(
                    f"Insufficient disk space. Need {required_space:,} bytes, "
                    f"have {available_space:,} bytes available"
                )

            total_lines = self.count_lines(input_path)
            
            # Detect number of columns in first row
            with open(input_path, 'r', encoding='utf-8') as f:
                first_row = next(csv.reader(f))
                expected_columns = len(first_row)
            
            with open(input_path, 'r', encoding='utf-8') as infile, \
                 open(temp_path, 'w', encoding='utf-8', newline='') as outfile:
                
                reader = csv.reader(
                    infile,
                    quoting=csv.QUOTE_MINIMAL,
                    doublequote=True,
                    escapechar='\\'
                )
                
                writer = csv.writer(
                    outfile,
                    quoting=csv.QUOTE_MINIMAL,
                    doublequote=True,
                    escapechar='\\'
                )

                chunk = []
                processed = 0
                
                with tqdm(total=total_lines, desc=f"Preprocessing {os.path.basename(input_path)}", unit="rows") as pbar:
                    for row in reader:
                        try:
                            # Skip empty rows
                            if not any(row):
                                continue
                                
                            processed_row = self.process_row(row, expected_columns)
                            chunk.append(processed_row)
                            processed += 1
                            
                            if len(chunk) >= chunk_size:
                                writer.writerows(chunk)
                                chunk.clear()
                                
                            pbar.update(1)
                            
                        except Exception as e:
                            logger.error(f"Error processing row: {row}")
                            logger.error(f"Error details: {str(e)}")
                            continue

                    # Write remaining rows
                    if chunk:
                        writer.writerows(chunk)
                        pbar.update(len(chunk))

            # Verify the processed file
            if processed == 0:
                raise RuntimeError(f"No valid rows processed in {input_path}")

            # Replace original file with processed file
            os.replace(temp_path, input_path)
            logger.info(f"Successfully preprocessed {processed:,} rows in {input_path}")
            
            return input_path

        except Exception as e:
            logger.error(f"Error preprocessing {input_path}")
            logger.error(f"Error details: {str(e)}")
            
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except Exception as cleanup_error:
                    logger.warning(f"Failed to remove temporary file: {cleanup_error}")
            raise