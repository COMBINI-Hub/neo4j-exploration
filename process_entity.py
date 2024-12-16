import csv
import re

# File paths
sql_file = "data/semmedVER43_2024_R_ENTITY.sql"  # Path to your .sql file
output_csv = "ENTITY.csv"   # Desired CSV file name

# Open the SQL file for reading
with open(sql_file, "r") as file:
    # Open the CSV file for writing
    with open(output_csv, "w", newline="") as csvfile:
        csv_writer = csv.writer(csvfile)

        # Iterate through the file line by line
        for line in file:
            # Match INSERT INTO statements with values
            if line.startswith("INSERT INTO `ENTITY` VALUES"):
                # Extract the values part of the INSERT INTO statement
                matches = re.findall(r"\((.*?)\)", line)
                for match in matches:
                    # Split values by comma, accounting for quoted strings
                    row = re.split(r",(?=(?:[^']*'[^']*')*[^']*$)", match)
                    # Clean up quotes and whitespace
                    cleaned_row = [col.strip(" '") for col in row]
                    # Write the row to the CSV
                    csv_writer.writerow(cleaned_row)

print(f"Data successfully extracted to {output_csv}")
