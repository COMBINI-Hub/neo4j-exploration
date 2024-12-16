import csv
import re

def convert_to_csv(input_file, output_file):
    # Read the input file
    with open(input_file, 'r') as f:
        content = f.read()
    
    # Find all matches between parentheses using regex
    pattern = r'\((.*?)\)'
    matches = re.findall(pattern, content)
    
    # Write to CSV file
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
        for match in matches:
            # Split the line by comma and strip whitespace
            row = [field.strip() for field in match.split(',')]
            writer.writerow(row)

# Usage
input_file = 'demo_data/entity.txt'  # Replace with your input file name
output_file = 'entity.csv'          # Replace with desired output file name

convert_to_csv(input_file, output_file)