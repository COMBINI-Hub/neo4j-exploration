#!/usr/bin/env python3
"""
Script to analyze connection types in SemMedDB connection files.
"""

import csv
from collections import Counter
import sys

def analyze_connection_types(file_path):
    """Analyze the connection types in a CSV file."""
    print(f"\n=== Analyzing {file_path} ===")
    
    connection_types = Counter()
    total_connections = 0
    
    try:
        with open(file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            
            for row_num, row in enumerate(reader, 1):
                if len(row) >= 3:
                    connection_type = row[2]  # Third column is the connection type
                    connection_types[connection_type] += 1
                    total_connections += 1
                
                # Progress update every 100,000 rows
                if row_num % 100000 == 0:
                    print(f"  Processed {row_num:,} rows...")
    
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return
    
    print(f"\nTotal connections: {total_connections:,}")
    print("\nConnection types found:")
    print("-" * 40)
    
    for conn_type, count in connection_types.most_common():
        percentage = (count / total_connections) * 100
        print(f"{conn_type:20} {count:10,} ({percentage:5.2f}%)")
    
    return connection_types

def main():
    """Main function to analyze all three connection files."""
    files = [
        'semmed_data/connections_1.csv',
        'semmed_data/connections_2.csv', 
        'semmed_data/connections.csv'
    ]
    
    all_connection_types = {}
    
    for file_path in files:
        try:
            connection_types = analyze_connection_types(file_path)
            if connection_types:
                all_connection_types[file_path] = connection_types
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    # Summary across all files
    print("\n" + "="*60)
    print("SUMMARY ACROSS ALL FILES")
    print("="*60)
    
    all_types = set()
    for connection_types in all_connection_types.values():
        all_types.update(connection_types.keys())
    
    print(f"\nAll unique connection types found: {sorted(all_types)}")
    
    # Show which files contain which connection types
    print("\nConnection type distribution across files:")
    print("-" * 60)
    
    for conn_type in sorted(all_types):
        print(f"\n{conn_type}:")
        for file_path, connection_types in all_connection_types.items():
            count = connection_types.get(conn_type, 0)
            if count > 0:
                print(f"  {file_path}: {count:,}")

if __name__ == "__main__":
    main()

