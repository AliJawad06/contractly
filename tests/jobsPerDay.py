import json
from datetime import datetime
from collections import Counter
import matplotlib.pyplot as plt

def load_jobs(json_file):
    """Load list of job objects from JSON file"""
    with open(json_file, "r", encoding="utf-8") as f:
        data =  json.load(f)
        return data
        

def get_jobs_per_day(jobs):
    """Return a Counter mapping date → job count"""
    dates = []
    for job in jobs:
        posted = job.get("posted", {})
        ts = posted.get("date_posted")

        # Skip missing or invalid entries
        if not ts:
            continue

        # Convert ms timestamp to date
        date = datetime.fromtimestamp(ts / 1000).date()
        dates.append(date)

    return Counter(dates)

def plot_jobs_per_day(job_counts):
    """Plot: X=date, Y=number of jobs"""
    dates = sorted(job_counts.keys())
    counts = [job_counts[d] for d in dates]

    plt.figure(figsize=(10, 5))
    plt.plot(dates, counts, marker="o", linestyle="-", color="tab:blue")
    plt.title("Jobs Posted Per Day")
    plt.xlabel("Date")
    plt.ylabel("Number of Jobs Posted")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_jobs_distribution(job_counts):
    """Plot: X=number of jobs, Y=number of days"""
    # Reverse the mapping: how many days had X jobs
    frequency = Counter(job_counts.values())
    x = sorted(frequency.keys())
    y = [frequency[i] for i in x]

    plt.figure(figsize=(8, 5))
    plt.bar(x, y, color="tab:green", edgecolor="black", alpha=0.8)
    plt.title("Distribution of Job Postings per Day")
    plt.xlabel("Number of Jobs Posted (per day)")
    plt.ylabel("Number of Days")
    plt.grid(True, linestyle="--", alpha=0.6, axis="y")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    json_file = "/Users/aj/Downloads/DhikrUp/contractJobs/jsonl/combined.json"
    jobs = load_jobs(json_file)
    job_counts = get_jobs_per_day(jobs)

    print("✅ Jobs per day (sample):")
    for date, count in sorted(job_counts.items())[:10]:
        print(f"{date}: {count} jobs")

    print("\n📊 Summary:")
    print(f"Total days recorded: {len(job_counts)}")
    print(f"Total jobs: {sum(job_counts.values())}")
    print(f"Average per day: {round(sum(job_counts.values()) / len(job_counts), 2)}")

    # --- Visualization ---
    plot_jobs_per_day(job_counts)
    plot_jobs_distribution(job_counts)
