#!/usr/bin/env python3
"""
Convert merged nodes and edges TSVs to Neo4j-admin compatible CSVs.
Preserves all original columns and adds Neo4j-admin import headers and columns.
"""
import sys
import pandas as pd
import argparse
import csv
from pathlib import Path

def clean_label(category):
    if pd.isna(category) or str(category).strip() == '':
        return 'UnknownCategory'
    if str(category).startswith('http'):
        return 'ExternalCategory'
    if ':' in str(category):
        return str(category).split(':', 1)[1]
    return str(category)

def clean_type(predicate):
    if pd.isna(predicate) or str(predicate).strip() == '':
        return 'RELATED_TO'
    s = str(predicate)
    if s.startswith('http') or '/' in s or '\\' in s:
        # Replace backslashes with slashes, remove trailing slashes, split, get last non-empty
        s = s.replace('\\', '/')
        s = s.rstrip('/')
        parts = [p for p in s.split('/') if p]
        if parts:
            return parts[-1].replace(':', '_').upper()
        else:
            return 'RELATED_TO'
    if ':' in s:
        return s.split(':', 1)[1].replace(':', '_').upper()
    return s.replace(':', '_').upper()

def process_nodes_tsv(input_path, output_path):
    df = pd.read_csv(input_path, sep='\t', dtype=str, keep_default_na=False)
    df[':LABEL'] = df['category'].apply(clean_label)
    df['id:ID'] = df['id']
    # Place id:ID and :LABEL at the front/back
    cols = ['id:ID'] + [c for c in df.columns if c not in ['id', 'id:ID', ':LABEL']] + [':LABEL']
    df[cols].to_csv(output_path, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote nodes CSV: {output_path}")

def process_edges_tsv(input_path, output_path):
    df = pd.read_csv(input_path, sep='\t', dtype=str, keep_default_na=False)
    df[':TYPE'] = df['predicate'].apply(clean_type)
    df[':START_ID'] = df['subject']
    df[':END_ID'] = df['object']
    # Place :START_ID, :END_ID, :TYPE at the front, rest as properties
    cols = [':START_ID', ':END_ID', ':TYPE'] + [c for c in df.columns if c not in [':START_ID', ':END_ID', ':TYPE']]
    df[cols].to_csv(output_path, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote edges CSV: {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Convert merged nodes and edges TSVs to Neo4j-admin compatible CSVs.")
    parser.add_argument('--nodes', default='kg_bioportal/import/merged_nodes.tsv', help='Input merged nodes TSV file')
    parser.add_argument('--edges', default='kg_bioportal/import/merged_edges.tsv', help='Input merged edges TSV file')
    parser.add_argument('--nodes_out', default='kg_bioportal/import/nodes_for_neo4j.csv', help='Output nodes CSV file')
    parser.add_argument('--edges_out', default='kg_bioportal/import/edges_for_neo4j.csv', help='Output edges CSV file')
    args = parser.parse_args()

    if not Path(args.nodes).exists():
        print(f"Nodes file not found: {args.nodes}")
        sys.exit(1)
    if not Path(args.edges).exists():
        print(f"Edges file not found: {args.edges}")
        sys.exit(1)

    process_nodes_tsv(args.nodes, args.nodes_out)
    process_edges_tsv(args.edges, args.edges_out)
    print("Done!")

if __name__ == "__main__":
    main() 