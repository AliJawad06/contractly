import os
import pandas as pd

# Folder containing the CSVs (change this if needed)
folder_path = "."

# Output filename
output_file = "combined.csv"

# List to store dataframes
dataframes = []

# Loop through all files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith(".csv") and filename != "companies.csv":
        file_path = os.path.join(folder_path, filename)
        print(f"Adding {filename}...")
        df = pd.read_csv(file_path)
        df["source_file"] = filename  # optional: track where each row came from
        dataframes.append(df)

# Concatenate all dataframes
if dataframes:
    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df.to_csv(output_file, index=False)
    print(f"Combined CSV saved to {output_file}")
else:
    print("No CSV files found to combine.")