import json
from collections import Counter
import matplotlib.pyplot as plt
import statistics

def get_output_tokens(jsonl_path):
    output_tokens = []
    
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            try:
                data = json.loads(line)
                tokens = (
                    data.get("response", {})
                        .get("body", {})
                        .get("usage", {})
                        .get("input_tokens")
                )
                if isinstance(tokens, int):
                    output_tokens.append(tokens)
            except json.JSONDecodeError:
                print(f"⚠️ Skipping invalid JSON on line {line_num}")
                continue

    return output_tokens


def plot_distribution(values):
    plt.figure(figsize=(10, 6))
    plt.hist(values, bins=30, edgecolor="black", alpha=0.7)
    plt.title("Distribution of Output Tokens per Job")
    plt.xlabel("Output Tokens")
    plt.ylabel("Frequency")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.show()


if __name__ == "__main__":
    jsonl_path = "/Users/aj/Downloads/DhikrUp/contractJobs/batch_output.jsonl"
    output_tokens = get_output_tokens(jsonl_path)

    print("Total responses processed:", len(output_tokens))
    print("Total output tokens:", sum(output_tokens))

    if output_tokens:
        print("\nSummary stats:")
        print("Average tokens:", round(statistics.mean(output_tokens)))
        print("Median tokens:", statistics.median(output_tokens))
        print("Max tokens:", max(output_tokens))
        print("Min tokens:", min(output_tokens))

        plot_distribution(output_tokens)
