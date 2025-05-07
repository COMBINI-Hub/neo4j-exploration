import csv
import os
from collections import defaultdict

def merge_duplicates(input_file, output_file):
    """
    Process a CSV file to add a frequency column and merge duplicate rows.
    Duplicates are identified by having the same values in the first three columns.
    """
    print(f"Processing {input_file}...")
    
    # Dictionary to store unique rows and their frequencies
    unique_rows = defaultdict(int)
    
    # Read the input CSV file
    with open(input_file, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        
        # Get the header row
        header = next(reader)
        
        # Process each row in the CSV
        for row in reader:
            if len(row) >= 3:
                # Use the first three columns as the key
                key = tuple(row[:3])
                # Increment the frequency for this key
                unique_rows[key] += 1
    
    print(f"Found {len(unique_rows)} unique relationships")
    
    # Write the results to the output CSV file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write the header row with the new frequency column
        new_header = header + ['frequency']
        writer.writerow(new_header)
        
        # Write each unique row with its frequency
        for key, frequency in unique_rows.items():
            # Convert the key back to a list and add the frequency
            row = list(key)
            # Add any additional columns from the original data if needed
            # If the original row had more than 3 columns, you might want to handle that here
            row.append(str(frequency))
            writer.writerow(row)
    
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    # Default file paths - adjust as needed
    input_file = "data/connections.csv"
    output_file = "data/connections_with_frequency.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found.")
        exit(1)
    
    merge_duplicates(input_file, output_file)

