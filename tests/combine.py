import os
import json
def combine_jsonl_files(folder_path, output_file):
    all_jobs = []
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for filename in os.listdir(folder_path):
            if filename.endswith('.json'):
                file_path = os.path.join(folder_path, filename)
                print(file_path)
                with open(file_path, 'r', encoding='utf-8') as infile:
                    data = json.load(infile)
                    jobs = data["jobs"]
                    for job in jobs:
                        all_jobs.append(job)
        json.dump(all_jobs, outfile, indent=2, ensure_ascii=False)

    print(f"✅ Combined all JSONL files into: {output_file}")

if __name__ == "__main__":
    folder_path = "/Users/aj/Downloads/DhikrUp/contractJobs/jsonl"
    output_file = os.path.join(folder_path, "combined.jsonl")
    combine_jsonl_files(folder_path, output_file)
