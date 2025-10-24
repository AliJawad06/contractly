import json
import tiktoken
from collections import Counter
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import statistics

def strip_html(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def num_tokens_from_string(string: str, model: str = "gpt-4o") -> int:
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")
    return len(encoding.encode(string))

def analyze_job_descriptions(json_file: str, model: str = "gpt-4o"):
    token_counts = []
    with open(json_file, encoding="utf-8") as f:
        data = json.load(f)
        jobs = data.get('jobs', [])
        for job in jobs:
            raw_text = job.get("description", "")
            clean_text = strip_html(raw_text)
            tokens = num_tokens_from_string(clean_text, model)
            token_counts.append(tokens)

    distribution = Counter(token_counts)
    total_tokens = sum(token_counts)
    return distribution, token_counts, total_tokens

def plot_distribution(token_counts):
    plt.figure(figsize=(10, 6))
    plt.hist(token_counts, bins=30, edgecolor="black", alpha=0.7)
    plt.title("Distribution of Token Counts in Job Descriptions")
    plt.xlabel("Number of Tokens")
    plt.ylabel("Frequency")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.show()

if __name__ == "__main__":
    json_file = "/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251003_175606.json"
    distribution, token_counts, total_tokens = analyze_job_descriptions(json_file)

    print("Token count distribution (sample):")
    for token_size, count in distribution.most_common(10):
        print(f"{token_size} tokens: {count} occurrences")

    print("\nTotal rows processed:", len(token_counts))
    print("Total tokens across all job descriptions:", total_tokens)

    print("\nSummary Stats:")
    print("Average tokens:", round(statistics.mean(token_counts)))
    print("Median tokens:", statistics.median(token_counts))
    print("Max tokens:", max(token_counts))
    print("Min tokens:", min(token_counts))

    plot_distribution(token_counts)
