import csv
from collections import Counter
import tiktoken
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import json

#6828343
#14782637
# --- Helper: strip HTML using BeautifulSoup ---
def strip_html(text: str) -> str:
    #if not text:
    #    return ""
    #soup = BeautifulSoup(text, "html.parser")
    return text #soup.get_text(separator=" ", strip=True)


# --- Helper: get token count ---
def num_tokens_from_string(string: str, model: str = "gpt-4o") -> int:
    encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(string))


# --- Main script ---
def analyze_job_descriptions(csv_file: str, model: str = "gpt-4o"):
    token_counts = []

    with open(csv_file, newline="", encoding="utf-8") as f:
        data = json.load(f)
        jobs = data['jobs']
       

        for job in jobs:
            raw_text = job.get("description", "")
            clean_text = strip_html(raw_text)
            tokens = num_tokens_from_string(clean_text, model)
            token_counts.append(tokens)

    # Distribution of token sizes
    distribution = Counter(token_counts)
    total_tokens = sum(token_counts)

    return distribution, token_counts, total_tokens


# --- Plotting ---
def plot_distribution(token_counts):
    plt.figure(figsize=(10, 6))
    plt.hist(token_counts, bins=30, edgecolor="black", alpha=0.7)
    plt.title("Distribution of Token Counts in Job Descriptions")
    plt.xlabel("Number of Tokens")
    plt.ylabel("Frequency")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.show()


if __name__ == "__main__":
    csv_file = "/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251003_175606.json"  # <-- change to your CSV path
    distribution, token_counts, total_tokens = analyze_job_descriptions(csv_file)

    print("Token count distribution (sample):")
    for token_size, count in distribution.most_common(10):
        print(f"{token_size} tokens: {count} occurrences")

    print("\nTotal rows processed:", len(token_counts))
    print("Total tokens across all job descriptions:", total_tokens)

    # Plot histogram
    plot_distribution(token_counts)
