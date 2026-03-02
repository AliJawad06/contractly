import json
import asyncio
from dateutil import parser
from bs4 import BeautifulSoup
from location_parser import location_parser, save_mappers, get_mapper_stats


async def combine_jsonl_files():
    """
    Combine job data from multiple sources, parse locations and titles,
    and save to combined files
    """
    # File paths
    jobsapi_path = "/Users/aj/Downloads/DhikrUp/contractJobs/day/jobsapi_day.json"
    jobdiva_path = "/Users/aj/Downloads/DhikrUp/contractJobs/day/jobdiva_day.json"
    combineday_path = "/Users/aj/Downloads/DhikrUp/contractJobs/day/day.json"
    all_combined_path = "all_combined.json"
    all_clean_path = "all_clean_combined.json"

    japi_jobs = []
    diva_jobs = []
    master_jobs = []
    all_clean_jobs = []
    
    
    try:
        # Read all input files
        with open(all_clean_path, 'r', encoding='utf-8') as clean_file:
            all_clean_jobs = json.load(clean_file)
        with open(jobsapi_path, 'r', encoding='utf-8') as japi_file:
            japi_data = json.load(japi_file)
            japi_jobs = japi_data.get("jobs", [])
        
        with open(jobdiva_path, "r", encoding='utf-8') as diva_file:
            diva_data = json.load(diva_file)
            diva_jobs = diva_data.get("jobs", [])
        
        with open(all_combined_path, 'r', encoding='utf-8') as combined_file:
            master_jobs = json.load(combined_file)
        
        print(f"Loaded {len(japi_jobs)} jobs from jobsapi")
        print(f"Loaded {len(diva_jobs)} jobs from jobdiva")
        
        # Combine all jobs from both sources
        all_jobs = diva_jobs + japi_jobs
        print(f"Total jobs to process: {len(all_jobs)}")
        
        # Process each job
        clean_jobs = []
        processed_count = 0
        error_count = 0
        
        for i, job in enumerate(all_jobs, 1):
            try:
                # Parse location and get category
                experience = ""
                experience_data = job.get("experience", {})
                if isinstance(experience_data, dict):
                    experience = (
                        experience_data.get("min_experience") or 
                        experience_data.get("experience") or 
                        "N/A"
                    )
                else:
                    experience = "N/A"
                
                # Clean description HTML
                description = job.get("description", "")
                if description and description != "N/A":
                    try:
                        description = BeautifulSoup(description, "html.parser").get_text()
                    except:
                        pass  # Keep original if parsing fails

                job["experience"] = experience
                job["description"] = description

                with open("/Users/aj/Downloads/DhikrUp/contractJobs/day/day.json", 'w+') as dayw:
                    current_jobs = json.load(dayw)
                    current_jobs = current_jobs + japi_jobs + diva_jobs
                    json.dump(current_jobs, dayw, indent=2)
            except Exception as e:
                error_count += 1
                print(f"Error processing job {i}: {e}")
                continue
            
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format - {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(combine_jsonl_files())