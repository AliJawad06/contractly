#!/usr/bin/env python3

import os
import csv
import json
from pathlib import Path

def process_csv_folders():
    # Get current directory
    current_dir = Path('.')
    
    # Iterate through all subdirectories
    for folder in current_dir.iterdir():
        if folder.is_dir():
            print(f"Processing folder: {folder.name}")
            
            # Find CSV files in the folder
            csv_files = list(folder.glob('*.csv'))
            
            if not csv_files:
                print(f"  No CSV files found in {folder.name}")
                continue
                
            # Process the first CSV file found
            csv_file = csv_files[0]
            print(f"  Found CSV: {csv_file.name}")
            
            try:
                with open(csv_file, 'r', newline='', encoding='utf-8') as file:
                    reader = csv.reader(file)
                    lines = list(reader)
                    
                    if len(lines) < 2:
                        print(f"  CSV file {csv_file.name} has less than 2 lines")
                        continue
                    
                    # Get the second line (index 1)
                    second_line = lines[1]
                    
                    # Get headers from first line for better JSON structure
                    headers = lines[0] if lines else []
                    
                    # Create JSON object
                    if headers and len(headers) == len(second_line):
                        # Create dictionary with headers as keys
                        json_data = dict(zip(headers, second_line))
                    else:
                        # Fallback: use array format
                        json_data = second_line
                    
                    # Write to last_job.json in the same folder
                    json_file_path = folder / 'last_job.json'
                    with open(json_file_path, 'w', encoding='utf-8') as json_file:
                        json.dump(json_data, json_file, indent=2, ensure_ascii=False)
                    
                    print(f"  Created: {json_file_path}")
                    
            except Exception as e:
                print(f"  Error processing {csv_file.name}: {e}")

if __name__ == "__main__":
    process_csv_folders()
    print("Done!")