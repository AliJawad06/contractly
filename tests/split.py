import json 
import os

def split(file_temp, meta, jobs):
    i = 0
    current_file = file_temp + "_s" + str(i) + ".jsonl"
    
    for job in jobs:
        # Check if current file exists and its size
        if os.path.exists(current_file) and os.path.getsize(current_file) >= 25165824:  # 24 MB
            i += 1
            current_file = file_temp + "_s" + str(i) + ".jsonl"
            print(f"Creating new file: {current_file}")
        
        # Write job to current file
        with open(current_file, "a") as w:
            json.dump(job, w)
            w.write("\n")
    
    print(f"\n✓ Split complete! Created {i + 1} file(s)")
    
    # Print file sizes
    for j in range(i + 1):
        filename = file_temp + "_s" + str(j) + ".jsonl"
        size_mb = os.path.getsize(filename) / (1024 * 1024)
        print(f"  {filename}: {size_mb:.2f} MB")

file_temp = "/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251023_194537"

print(f"Reading source file: {file_temp}.json")

with open(file_temp + ".json", "r") as srcW:
    data = json.load(srcW)
    meta = data["metadata"]
    jobs = data["jobs"]
    print(f"Total jobs to process: {len(jobs)}")
    print(f"Starting split (max 24 MB per file)...\n")
    split(file_temp, meta, jobs)