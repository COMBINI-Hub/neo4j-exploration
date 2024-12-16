import os
from pathlib import Path
from multiprocessing import Pool
from typing import Tuple

def process_file(args: Tuple[str, str, int]) -> str:
    """
    Process a single file by sampling n lines.
    
    Args:
        args: Tuple containing (source_path, dest_path, n_lines)
    Returns:
        str: Filename that was processed
    """
    source_path, dest_path, n_lines = args
    
    with open(source_path, 'r', encoding='utf-8') as source, \
         open(dest_path, 'w', encoding='utf-8') as dest:
        # Copy first n_lines lines
        for i, line in enumerate(source):
            if i >= n_lines:
                break
            dest.write(line)
    
    return os.path.basename(source_path)

def sample_files(source_dir: str, dest_dir: str, n_lines: int = 100_000, n_workers: int = None):
    """
    Sample the first n lines from each CSV file in source_dir and save to dest_dir.
    Uses multiprocessing for parallel processing.
    
    Args:
        source_dir: Directory containing the original data files
        dest_dir: Directory where sampled files will be stored
        n_lines: Number of lines to sample from each file (default: 100,000)
        n_workers: Number of parallel workers (default: CPU count)
    """
    # Create destination directory if it doesn't exist
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    
    # Prepare arguments for parallel processing
    csv_files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
    process_args = [
        (
            os.path.join(source_dir, filename),
            os.path.join(dest_dir, filename),
            n_lines
        )
        for filename in csv_files
    ]
    
    # Process files in parallel
    with Pool(processes=n_workers) as pool:
        for filename in pool.imap_unordered(process_file, process_args):
            print(f"Processed: {filename}")

def main():
    # Configure directories
    SOURCE_DIR = "data"  # Directory with your full dataset
    DEMO_DIR = "demo_data"  # Directory for sampled data
    
    try:
        sample_files(SOURCE_DIR, DEMO_DIR)
        print("Successfully created sample files!")
        
    except Exception as e:
        print(f"Error creating sample files: {str(e)}")
        raise

if __name__ == "__main__":
    main()