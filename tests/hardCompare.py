import json
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from typing import Dict, List, Set
from collections import defaultdict

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (16, 12)

def extract_skills(parsed_data: Dict) -> Dict[str, Set[str]]:
    """Extract all skills from a parsed job posting."""
    calendar = parsed_data.get("calendar_event", {})
    
    skills = {
        "required_hard": set(calendar.get("required", {}).get("hardSkills", [])),
        "required_soft": set(calendar.get("required", {}).get("softSkills", [])),
        "preferred_hard": set(calendar.get("preferred", {}).get("hardSkills", [])),
        "preferred_soft": set(calendar.get("preferred", {}).get("softSkills", [])),
        "certs": set(calendar.get("certsLicences", [])),
        "categories": set(calendar.get("categorys", []))
    }
    
    return skills

def calculate_similarity(base_skills: Dict[str, Set], compare_skills: Dict[str, Set]) -> Dict:
    """Calculate Jaccard similarity for each skill category."""
    similarities = {}
    
    for category in base_skills.keys():
        base_set = base_skills[category]
        compare_set = compare_skills[category]
        
        if len(base_set) == 0 and len(compare_set) == 0:
            similarity = 1.0
        elif len(base_set) == 0 or len(compare_set) == 0:
            similarity = 0.0
        else:
            intersection = len(base_set & compare_set)
            union = len(base_set | compare_set)
            similarity = intersection / union if union > 0 else 0.0
        
        similarities[category] = {
            "similarity": similarity,
            "base_count": len(base_set),
            "compare_count": len(compare_set),
            "intersection": len(base_set & compare_set),
            "missing_from_compare": list(base_set - compare_set),
            "extra_in_compare": list(compare_set - base_set)
        }
    
    return similarities

def calculate_overall_similarity(similarities: Dict) -> float:
    """Calculate weighted overall similarity."""
    weights = {
        "required_hard": 0.3,
        "required_soft": 0.2,
        "preferred_hard": 0.2,
        "preferred_soft": 0.1,
        "certs": 0.1,
        "categories": 0.1
    }
    
    weighted_sum = sum(
        similarities[cat]["similarity"] * weights[cat]
        for cat in similarities.keys()
    )
    
    return weighted_sum

# Load and process data
file_path = "/Users/aj/Downloads/DhikrUp/contractJobs/compare_all_output.json"

with open(file_path, "r") as r:
    data = json.load(r)

results = []
model_names = ["five", "gemini_2.5_pro", "mini", "nano"]

for idx, job in enumerate(data):
    try:
        base_data = json.loads(job["base"])
        base_skills = extract_skills(base_data)
        
        job_results = {
            "job_index": idx,
            "comparisons": {}
        }
        
        for model_name in model_names:
            if model_name in job:
                model_data = json.loads(job[model_name])
                model_skills = extract_skills(model_data)
                
                similarities = calculate_similarity(base_skills, model_skills)
                overall = calculate_overall_similarity(similarities)
                
                job_results["comparisons"][model_name] = {
                    "overall_similarity": overall,
                    "details": similarities
                }
        
        results.append(job_results)
        
    except Exception as e:
        print(f"Error processing job {idx}: {e}")

# Prepare data for visualization
overall_scores = {model: [] for model in model_names}
category_scores = {model: defaultdict(list) for model in model_names}

for result in results:
    for model in model_names:
        if model in result["comparisons"]:
            overall_scores[model].append(result["comparisons"][model]["overall_similarity"])
            
            for category, details in result["comparisons"][model]["details"].items():
                category_scores[model][category].append(details["similarity"])

# Create visualizations
fig = plt.figure(figsize=(20, 12))

# 1. Overall Similarity Comparison (Box Plot)
ax1 = plt.subplot(2, 3, 1)
box_data = [overall_scores[model] for model in model_names]
bp = ax1.boxplot(box_data, labels=model_names, patch_artist=True)
colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A']
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)
ax1.set_ylabel('Overall Similarity Score', fontsize=12)
ax1.set_title('Overall Similarity Distribution', fontsize=14, fontweight='bold')
ax1.set_ylim(0, 1)
ax1.grid(axis='y', alpha=0.3)

# 2. Average Similarity by Model (Bar Chart)
ax2 = plt.subplot(2, 3, 2)
avg_scores = [np.mean(overall_scores[model]) for model in model_names]
bars = ax2.bar(model_names, avg_scores, color=colors, alpha=0.8, edgecolor='black')
ax2.set_ylabel('Average Similarity', fontsize=12)
ax2.set_title('Average Overall Similarity by Model', fontsize=14, fontweight='bold')
ax2.set_ylim(0, 1)
for i, (bar, score) in enumerate(zip(bars, avg_scores)):
    ax2.text(i, score + 0.02, f'{score:.3f}', ha='center', fontweight='bold')
ax2.grid(axis='y', alpha=0.3)

# 3. Category-wise Similarity Heatmap
ax3 = plt.subplot(2, 3, 3)
categories = list(category_scores[model_names[0]].keys())
heatmap_data = []
for model in model_names:
    row = [np.mean(category_scores[model][cat]) for cat in categories]
    heatmap_data.append(row)

sns.heatmap(heatmap_data, annot=True, fmt='.3f', cmap='YlGnBu', 
            xticklabels=[c.replace('_', '\n') for c in categories],
            yticklabels=model_names, cbar_kws={'label': 'Similarity'},
            ax=ax3, vmin=0, vmax=1)
ax3.set_title('Category-wise Average Similarity', fontsize=14, fontweight='bold')

# 4. Skill Count Comparison (Required Hard Skills)
ax4 = plt.subplot(2, 3, 4)
skill_counts = {model: [] for model in model_names}
base_counts = []

for result in results:
    for model in model_names:
        if model in result["comparisons"]:
            count = result["comparisons"][model]["details"]["required_hard"]["compare_count"]
            skill_counts[model].append(count)
            if model == model_names[0]:
                base_counts.append(result["comparisons"][model]["details"]["required_hard"]["base_count"])

x = np.arange(len(results))
width = 0.15
for i, model in enumerate(model_names):
    ax4.bar(x + i*width, skill_counts[model], width, label=model, alpha=0.8)

if base_counts:
    ax4.plot(x + width*1.5, base_counts, 'k--', linewidth=2, label='Base', marker='o')

ax4.set_xlabel('Job Index', fontsize=12)
ax4.set_ylabel('Number of Required Hard Skills', fontsize=12)
ax4.set_title('Required Hard Skills Count Comparison', fontsize=14, fontweight='bold')
ax4.legend()
ax4.grid(axis='y', alpha=0.3)

# 5. Missing vs Extra Skills (Stacked Bar)
ax5 = plt.subplot(2, 3, 5)
missing_counts = []
extra_counts = []

for model in model_names:
    missing = []
    extra = []
    for result in results:
        if model in result["comparisons"]:
            total_missing = sum(
                len(result["comparisons"][model]["details"][cat]["missing_from_compare"])
                for cat in categories
            )
            total_extra = sum(
                len(result["comparisons"][model]["details"][cat]["extra_in_compare"])
                for cat in categories
            )
            missing.append(total_missing)
            extra.append(total_extra)
    
    missing_counts.append(np.mean(missing))
    extra_counts.append(np.mean(extra))

x_pos = np.arange(len(model_names))
ax5.bar(x_pos, missing_counts, label='Missing from Model', color='#E74C3C', alpha=0.8)
ax5.bar(x_pos, extra_counts, bottom=missing_counts, label='Extra in Model', color='#3498DB', alpha=0.8)
ax5.set_xticks(x_pos)
ax5.set_xticklabels(model_names)
ax5.set_ylabel('Average Number of Skills', fontsize=12)
ax5.set_title('Average Missing vs Extra Skills', fontsize=14, fontweight='bold')
ax5.legend()
ax5.grid(axis='y', alpha=0.3)

# 6. Consistency Score (Standard Deviation)
ax6 = plt.subplot(2, 3, 6)
std_scores = [np.std(overall_scores[model]) for model in model_names]
bars = ax6.bar(model_names, std_scores, color=colors, alpha=0.8, edgecolor='black')
ax6.set_ylabel('Standard Deviation', fontsize=12)
ax6.set_title('Model Consistency (Lower is More Consistent)', fontsize=14, fontweight='bold')
for i, (bar, score) in enumerate(zip(bars, std_scores)):
    ax6.text(i, score + 0.005, f'{score:.3f}', ha='center', fontweight='bold')
ax6.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('/Users/aj/Downloads/DhikrUp/contractJobs/similarity_visualization.png', 
            dpi=300, bbox_inches='tight')
print("Visualization saved to: /Users/aj/Downloads/DhikrUp/contractJobs/similarity_visualization.png")

# Print summary statistics
print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)

for model in model_names:
    scores = overall_scores[model]
    if scores:
        print(f"\n{model.upper()}:")
        print(f"  Average Similarity: {np.mean(scores):.3f}")
        print(f"  Std Dev: {np.std(scores):.3f}")
        print(f"  Min: {min(scores):.3f}, Max: {max(scores):.3f}")
        print(f"  Median: {np.median(scores):.3f}")

plt.show()