import csv
import re
import random
from tqdm import tqdm

def clean_value(value):
    # Remove leading/trailing quotes and spaces
    value = value.strip("' ")
    # Replace escaped single quotes with single quotes
    value = value.replace("\\'", "'")
    return value

def create_sample_file(input_file, sample_file, sample_size=10000):
    """
    Create a sample file from the input SQL file
    Sample size is the number of INSERT statements to include
    """
    print(f"Creating sample file with {sample_size} INSERT statements...")
    
    # First pass: count total INSERT statements
    total_inserts = 0
    print("Counting total INSERT statements...")
    with open(input_file, 'r') as f:
        for line in tqdm(f):
            if line.startswith("INSERT INTO `ENTITY` VALUES"):
                total_inserts += 1
    
    # Calculate sampling rate
    sampling_rate = min(1.0, sample_size / total_inserts)
    
    # Second pass: create sample file
    print("\nCreating sample file...")
    sampled_count = 0
    with open(input_file, 'r') as f:
        with open(sample_file, 'w') as out:
            with tqdm(total=sample_size) as pbar:
                for line in f:
                    if line.startswith("INSERT INTO `ENTITY` VALUES"):
                        if random.random() < sampling_rate:
                            out.write(line)
                            sampled_count += 1
                            pbar.update(1)
                            if sampled_count >= sample_size:
                                break
                    else:
                        # Copy non-INSERT lines (like CREATE TABLE statements)
                        out.write(line)
    
    print(f"Sample file created at {sample_file}")

def process_sql_to_csv(sql_file, output_csv):
    """Process SQL file to CSV format"""
    print(f"Processing {sql_file} to CSV...")
    
    # First count the number of INSERT statements for the progress bar
    total_inserts = 0
    print("Counting INSERT statements...")
    with open(sql_file, 'r') as f:
        for line in tqdm(f):
            if line.startswith("INSERT INTO `ENTITY` VALUES"):
                total_inserts += 1
    
    print(f"\nConverting {total_inserts} INSERT statements to CSV...")
    with open(sql_file, "r") as file:
        with open(output_csv, "w", newline="") as csvfile:
            csv_writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            
            # Initialize progress bar
            pbar = tqdm(total=total_inserts)
            
            for line in file:
                if line.startswith("INSERT INTO `ENTITY` VALUES"):
                    # Extract content between parentheses, handling nested parentheses
                    values_part = line[line.find("("):]
                    matches = []
                    stack = []
                    start = -1
                    
                    for i, char in enumerate(values_part):
                        if char == '(' and not stack:
                            start = i
                        elif char == '(':
                            stack.append(char)
                        elif char == ')':
                            if stack:
                                stack.pop()
                            elif start != -1:
                                matches.append(values_part[start+1:i])
                                start = -1

                    for match in matches:
                        try:
                            # Split on commas that are not within quotes
                            row = re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", match)
                            # Clean each value
                            cleaned_row = [clean_value(col) for col in row]
                            # Write the row to the CSV
                            csv_writer.writerow(cleaned_row)
                        except Exception as e:
                            print(f"\nError processing row: {match}")
                            print(f"Error details: {str(e)}")
                            continue
                    
                    pbar.update(1)
            
            pbar.close()

    print(f"\nData successfully extracted to {output_csv}")

if __name__ == "__main__":
    # File paths
    input_sql = "data/semmedVER43_2024_R_ENTITY.sql"  # Original SQL file
    sample_sql = "data/sample_ENTITY.sql"              # Sampled SQL file
    sample_csv = "data/sample_ENTITY.csv"              # Final CSV output
    
    # Create sample file first
    create_sample_file(input_sql, sample_sql, sample_size=5000)
    
    # Process sample file to CSV
    process_sql_to_csv(sample_sql, sample_csv)