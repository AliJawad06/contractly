import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# === CONFIGURATION ===
INPUT_FILE = "/Users/aj/Downloads/DhikrUp/contractJobs/compare_all_output.json"
OUTPUT_FILE = "model_similarity_scores.csv"

# === HELPER FUNCTIONS ===
def jaccard_similarity(a, b):
    """Compute Jaccard similarity between two lists (case-insensitive)."""
    set_a = set([x.strip().lower() for x in a]) if a else set()
    set_b = set([x.strip().lower() for x in b]) if b else set()
    if not set_a and not set_b:
        return 1.0
    return len(set_a & set_b) / len(set_a | set_b)

def extract_fields(obj):
    """Extract comparable lists safely from a calendar_event object."""
    if not isinstance(obj, dict):
        return {"hardSkills": [], "softSkills": [], "certsLicences": [], "categorys": []}
    event = obj.get("calendar_event", {})
    return {
        "hardSkills": event.get("required", {}).get("hardSkills", []),
        "softSkills": event.get("required", {}).get("softSkills", []),
        "certsLicences": event.get("certsLicences", []),
        "categorys": event.get("categorys", []),
    }

# === MAIN PROCESSING ===
rows = []
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)
    for job in data:
        
        try:
            base = data["base"]
        except Exception as e:
            print(f"⚠️ Could not parse 'base' on line {i}: {e}")
            continue

        models = {}
        for k, v in data.items():
            if k in ["description", "base"]:
                continue
            try:
                models[k] = json.loads(v)
            except Exception:
                continue

        base_fields = extract_fields(base)
        for model_name, model_obj in models.items():
            model_fields = extract_fields(model_obj)
            similarities = {
                field: jaccard_similarity(base_fields[field], model_fields[field])
                for field in base_fields
            }
            similarities["overall"] = sum(similarities.values()) / len(similarities)
            similarities["model"] = model_name
            rows.append(similarities)

# === BUILD DATAFRAME SAFELY ===
df = pd.DataFrame(rows)
print("✅ Columns in DataFrame:", df.columns.tolist())

expected_cols = ["model", "hardSkills", "softSkills", "certsLicences", "categorys", "overall"]
for col in expected_cols:
    if col not in df.columns:
        df[col] = None

df = df[expected_cols]
df.to_csv(OUTPUT_FILE, index=False)

# === VISUALIZATIONS ===
if not df.empty:
    sns.set(style="whitegrid", font_scale=1.1)

    # --- 1. Bar Chart for Overall Similarity ---
    plt.figure(figsize=(8, 5))
    sns.barplot(x="model", y="overall", data=df, palette="crest", errorbar=None)
    plt.title("Overall Similarity to Base (Jaccard Index)", fontsize=14, weight="bold")
    plt.ylabel("Average Similarity (0–1)")
    plt.xlabel("Model")
    plt.ylim(0, 1)
    plt.tight_layout()
    plt.show()

    # --- 2. Heatmap for Per-Field Similarity ---
    pivot = df.groupby("model")[["hardSkills", "softSkills", "certsLicences", "categorys"]].mean()
    plt.figure(figsize=(7, 4))
    sns.heatmap(pivot, annot=True, cmap="Blues", fmt=".2f", cbar_kws={"label": "Jaccard Similarity"})
    plt.title("Per-Field Similarity vs Base", fontsize=14, weight="bold")
    plt.ylabel("Model")
    plt.xlabel("Field")
    plt.tight_layout()
    plt.show()

print(f"✅ Done! Results saved to {OUTPUT_FILE}")
