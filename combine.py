import os
import pandas as pd

# Folder containing the CSVs (change this if needed)
folder_path = "/Users/aj/Downloads/DhikrUp/contractJobs/csv"

# Output filename
output_file = "combined.csv"

# List to store dataframes
dataframes = []

# Loop through all files in the folder
for root, dirs, filenames in os.walk(folder_path):
    for filename in filenames:
        print(filename)
        if filename.endswith(".csv") and filename != "companies.csv":
            company = filename.replace(".csv","")
            file_path = os.path.join(folder_path,company,filename)
            print(f"Adding {filename}...")
            print(file_path)
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