import re
import csv
import traceback
from collections import defaultdict, Counter
from bs4 import BeautifulSoup
import json

def split_sentences(text):
    """Split text into clean, normalized sentences."""
    parts = re.split(r'(?<=[.!?])\s+|\n+', text.strip())
    cleaned = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        p = re.sub(r'\s+', ' ', p)    # collapse whitespace
        p = p.lower()
        p = p.rstrip('.!?')           # normalize punctuation
        p = p.strip(' "\'–—-')        # remove quotes/dashes
        if p:
            cleaned.append(p)
    return cleaned

def find_repeating_sentences(text_list):
    """Return repeating sentences with document frequency and occurrence counts."""
    sentence_docs = defaultdict(set)
    sentence_total = Counter()

    for idx, text in enumerate(text_list):
        if not text:
            continue
        sentences = split_sentences(text)
        unique_sentences = set(sentences)
        for s in unique_sentences:
            sentence_docs[s].add(idx)
        for s in sentences:
            sentence_total[s] += 1

    repeats = []
    for s, docset in sentence_docs.items():
        if len(docset) > 1:
            repeats.append({
                "sentence": s,
                "count_in_docs": len(docset),
                "total_occurrences": sentence_total[s],
                "doc_indices": ";".join(map(str, sorted(docset)))
            })

    # Sort by number of docs, then total occurrences
    repeats.sort(key=lambda x: (x["count_in_docs"], x["total_occurrences"]), reverse=True)
    return repeats

# ---------- main ----------
input_path = '/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251003_175606.json'
output_path = 'repeats.csv'

try:
    with open(input_path, 'r', encoding='utf-8') as r:
        data = json.load(r)

    jobs = data.get('jobs') if isinstance(data, dict) else data
    texts = []

    for job in jobs:
        text = job.get('description') or job.get('job_description') or ''
        if not text:
            continue
        soup = BeautifulSoup(text, 'html.parser')
        clean = soup.get_text(separator=' ', strip=True)
        texts.append(clean)

    repeats = find_repeating_sentences(texts)

    # 💾 Write to CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["sentence", "count_in_docs", "total_occurrences", "doc_indices"])
        writer.writeheader()
        writer.writerows(repeats)

    print(f"✅ Saved {len(repeats)} repeating sentences to {output_path}")
    # Show top 5 preview
    for i, row in enumerate(repeats[:5]):
        print(f"{i+1}. ({row['count_in_docs']} docs) {row['sentence'][:100]}...")

except Exception:
    traceback.print_exc()
    print("❌ Error reading or processing file.")
