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
 
                res = await location_parser(job)
                
                category = res.get("category", "N/A")
                locations = res.get("locations", [job.get("location", "N/A")])
                
                # Use first location if multiple returned
                location = locations[0] if locations else job.get("location", "N/A")
                
                # Parse posted date
                posted_date_str = job.get("posted", {}).get("date_postedStr", "")
                try:
                    posted_str = parser.parse(posted_date_str).date().isoformat()
                except:
                    posted_str = posted_date_str
                
                # Extract experience

                
                # Build clean job object
                clean_job = {
                    "title": job.get("title", "N/A"),
                    "category": category,
                    "location": location,
                    "posted": job.get("posted", {}).get("date_posted", "N/A"),
                    "url": job.get("apply") or job.get("url", "N/A"),
                    "postedStr": posted_str,
                    "startStr": job.get("start", {}).get("start_dateStr", "N/A"),
                    "start": job.get("start", {}).get("start_date", "N/A"),
                    "description": description,
                    "experience": job.get("experience"),
                    "remote": job.get("remote", "N/A")
                }
                
                clean_jobs.append(clean_job)
                all_clean_jobs.append(clean_job)
                processed_count += 1
                
                if i % 10 == 0:
                    print(f"Processed {i}/{len(all_jobs)} jobs...")
                
            except Exception as e:
                error_count += 1
                print(f"Error processing job {i}: {e}")
                continue
        
        print(f"\nProcessing complete:")
        print(f"  Successfully processed: {processed_count}")
        print(f"  Errors: {error_count}")
        
        # Save mapper data after all processing
        save_mappers()
        
        # Print mapper statistics
        stats = get_mapper_stats()
        print(f"\nMapper statistics:")
        print(f"  Locations cached: {stats['locations_cached']}")
        print(f"  Titles cached: {stats['titles_cached']}")
        
        # Add new jobs to master list
        master_jobs.extend(all_jobs)
        
        # Save combined raw jobs
        with open(all_combined_path, 'w', encoding='utf-8') as combined_file:
            json.dump(master_jobs, combined_file, indent=2, ensure_ascii=False)
        print(f"\nSaved {len(master_jobs)} total jobs to {all_combined_path}")
        
        # Save clean daily jobs
        with open(combineday_path, "w", encoding='utf-8') as day_file, open(all_clean_path, "w", encoding ='utf-8') as clean_file:
            json.dump(clean_jobs, day_file, indent=2, ensure_ascii=False)
            json.dump(all_clean_jobs, clean_file, indent=2, ensure_ascii=False)

        print(f"Saved {len(clean_jobs)} clean jobs to {combineday_path}")
        
        print("\n✅ Combined all JSONL files successfully")
        
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