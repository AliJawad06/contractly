import json
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from typing import Dict, Set, List
from collections import defaultdict
from matplotlib_venn import venn2

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (26, 16)

def extract_all_skills(parsed_data: Dict) -> Set[str]:
    """Extract all skills (hard + soft, required + preferred) into one set."""
    calendar = parsed_data.get("calendar_event", {})
    required = calendar.get("required", {})
    preferred = calendar.get("preferred", {})
    
    all_skills = (
        set(required.get("hardSkills", [])) |
        set(required.get("softSkills", [])) |
        set(preferred.get("hardSkills", [])) |
        set(preferred.get("softSkills", []))
    )
    
    return all_skills

def analyze_overlap(base_skills: Set[str], model_skills: Set[str]) -> Dict:
    """Analyze the overlap between base and model skills."""
    intersection = base_skills & model_skills
    only_in_base = base_skills - model_skills
    only_in_model = model_skills - base_skills
    
    total_unique = len(base_skills | model_skills)
    
    return {
        "intersection": intersection,
        "only_in_base": only_in_base,
        "only_in_model": only_in_model,
        "intersection_count": len(intersection),
        "only_in_base_count": len(only_in_base),
        "only_in_model_count": len(only_in_model),
        "base_total": len(base_skills),
        "model_total": len(model_skills),
        "jaccard_similarity": len(intersection) / total_unique if total_unique > 0 else 0,
        "recall": len(intersection) / len(base_skills) if len(base_skills) > 0 else 0,  # How many base skills were found
        "precision": len(intersection) / len(model_skills) if len(model_skills) > 0 else 0,  # How many model skills are correct
    }

# Load and process data
file_path = "/Users/aj/Downloads/DhikrUp/contractJobs/4.1msample.json"

with open(file_path, "r") as r:
    data = json.load(r)

model_names = ["five", "mini", "nano", "nano_alt", "mini_alt", "4.1", "4.1m"]
all_results = []

for idx, job in enumerate(data):
    try:
        base_data = json.loads(job["base"])
        base_skills = extract_all_skills(base_data)
        
        job_result = {
            "job_index": idx,
            "base_skill_count": len(base_skills),
            "models": {}
        }
        
        for model_name in model_names:
            if model_name in job:
                model_data = json.loads(job[model_name])
                model_skills = extract_all_skills(model_data)
                
                overlap_analysis = analyze_overlap(base_skills, model_skills)
                job_result["models"][model_name] = overlap_analysis
        
        all_results.append(job_result)
        
    except Exception as e:
        print(f"Error processing job {idx}: {e}")

# Aggregate statistics
stats = {model: {
    "jaccard": [],
    "recall": [],
    "precision": [],
    "intersection_counts": [],
    "only_base_counts": [],
    "only_model_counts": [],
    "base_totals": [],
    "model_totals": []
} for model in model_names}

for result in all_results:
    for model in model_names:
        if model in result["models"]:
            m = result["models"][model]
            stats[model]["jaccard"].append(m["jaccard_similarity"])
            stats[model]["recall"].append(m["recall"])
            stats[model]["precision"].append(m["precision"])
            stats[model]["intersection_counts"].append(m["intersection_count"])
            stats[model]["only_base_counts"].append(m["only_in_base_count"])
            stats[model]["only_model_counts"].append(m["only_in_model_count"])
            stats[model]["base_totals"].append(m["base_total"])
            stats[model]["model_totals"].append(m["model_total"])

# Create visualizations
fig = plt.figure(figsize=(22, 16))
colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#9B59B6', '#E67E22', "#0004FB"]

# 1. Jaccard Similarity (Intersection / Union)
ax1 = plt.subplot(3, 4, 1)
avg_jaccard = [np.mean(stats[model]["jaccard"]) for model in model_names]
bars = ax1.bar(model_names, avg_jaccard, color=colors, alpha=0.8, edgecolor='black')
ax1.set_ylabel('Jaccard Similarity', fontsize=12)
ax1.set_title('Average Jaccard Similarity\n(Intersection / Union)', fontsize=13, fontweight='bold')
ax1.set_ylim(0, 1)
for i, (bar, val) in enumerate(zip(bars, avg_jaccard)):
    ax1.text(i, val + 0.02, f'{val:.3f}', ha='center', fontweight='bold')
ax1.grid(axis='y', alpha=0.3)

# 2. Recall (What % of base skills were found)
ax2 = plt.subplot(3, 4, 2)
avg_recall = [np.mean(stats[model]["recall"]) for model in model_names]
bars = ax2.bar(model_names, avg_recall, color=colors, alpha=0.8, edgecolor='black')
ax2.set_ylabel('Recall', fontsize=12)
ax2.set_title('Average Recall\n(Found Base Skills / Total Base Skills)', fontsize=13, fontweight='bold')
ax2.set_ylim(0, 1)
for i, (bar, val) in enumerate(zip(bars, avg_recall)):
    ax2.text(i, val + 0.02, f'{val:.1%}', ha='center', fontweight='bold')
ax2.grid(axis='y', alpha=0.3)

# 3. Precision (What % of model skills are in base)
ax3 = plt.subplot(3, 4, 3)
avg_precision = [np.mean(stats[model]["precision"]) for model in model_names]
bars = ax3.bar(model_names, avg_precision, color=colors, alpha=0.8, edgecolor='black')
ax3.set_ylabel('Precision', fontsize=12)
ax3.set_title('Average Precision\n(Correct Model Skills / Total Model Skills)', fontsize=13, fontweight='bold')
ax3.set_ylim(0, 1)
for i, (bar, val) in enumerate(zip(bars, avg_precision)):
    ax3.text(i, val + 0.02, f'{val:.1%}', ha='center', fontweight='bold')
ax3.grid(axis='y', alpha=0.3)

# 4. F1 Score (Harmonic mean of Precision and Recall)
ax4 = plt.subplot(3, 4, 4)
f1_scores = []
for model in model_names:
    p = np.mean(stats[model]["precision"])
    r = np.mean(stats[model]["recall"])
    f1 = 2 * (p * r) / (p + r) if (p + r) > 0 else 0
    f1_scores.append(f1)

bars = ax4.bar(model_names, f1_scores, color=colors, alpha=0.8, edgecolor='black')
ax4.set_ylabel('F1 Score', fontsize=12)
ax4.set_title('F1 Score\n(Harmonic Mean of Precision & Recall)', fontsize=13, fontweight='bold')
ax4.set_ylim(0, 1)
for i, (bar, val) in enumerate(zip(bars, f1_scores)):
    ax4.text(i, val + 0.02, f'{val:.3f}', ha='center', fontweight='bold')
ax4.grid(axis='y', alpha=0.3)

# 5. Stacked Bar: Skills Distribution
ax5 = plt.subplot(3, 4, 5)
avg_intersection = [np.mean(stats[model]["intersection_counts"]) for model in model_names]
avg_only_base = [np.mean(stats[model]["only_base_counts"]) for model in model_names]
avg_only_model = [np.mean(stats[model]["only_model_counts"]) for model in model_names]

x = np.arange(len(model_names))
width = 0.6

p1 = ax5.bar(x, avg_intersection, width, label='In Both (Intersection)', color='#2ECC71', alpha=0.8)
p2 = ax5.bar(x, avg_only_base, width, bottom=avg_intersection, label='Only in Base (Missing)', color='#E74C3C', alpha=0.8)
p3 = ax5.bar(x, avg_only_model, width, bottom=np.array(avg_intersection)+np.array(avg_only_base), 
             label='Only in Model (Extra)', color='#3498DB', alpha=0.8)

ax5.set_ylabel('Average Skill Count', fontsize=12)
ax5.set_title('Skills Distribution Breakdown', fontsize=13, fontweight='bold')
ax5.set_xticks(x)
ax5.set_xticklabels(model_names)
ax5.legend(loc='upper left', fontsize=9)
ax5.grid(axis='y', alpha=0.3)

# 6. Box Plot: Jaccard Similarity Distribution
ax6 = plt.subplot(3, 4, 6)
box_data = [stats[model]["jaccard"] for model in model_names]
bp = ax6.boxplot(box_data, labels=model_names, patch_artist=True)
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)
ax6.set_ylabel('Jaccard Similarity', fontsize=12)
ax6.set_title('Jaccard Similarity Distribution', fontsize=13, fontweight='bold')
ax6.set_ylim(0, 1)
ax6.grid(axis='y', alpha=0.3)

# 7. Box Plot: Recall Distribution
ax7 = plt.subplot(3, 4, 7)
box_data = [stats[model]["recall"] for model in model_names]
bp = ax7.boxplot(box_data, labels=model_names, patch_artist=True)
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)
ax7.set_ylabel('Recall', fontsize=12)
ax7.set_title('Recall Distribution', fontsize=13, fontweight='bold')
ax7.set_ylim(0, 1)
ax7.grid(axis='y', alpha=0.3)

# 8. Skill Count Comparison
ax8 = plt.subplot(3, 4, 8)
avg_base = np.mean([r["base_skill_count"] for r in all_results])
avg_model_counts = [np.mean(stats[model]["model_totals"]) for model in model_names]

x_pos = np.arange(len(model_names) + 1)
bars = ax8.bar(x_pos, [avg_base] + avg_model_counts, 
               color=['#34495E'] + colors, alpha=0.8, edgecolor='black')
ax8.set_xticks(x_pos)
ax8.set_xticklabels(['Base'] + model_names)
ax8.set_ylabel('Average Total Skills', fontsize=12)
ax8.set_title('Average Total Skills Count', fontsize=13, fontweight='bold')
for i, (bar, count) in enumerate(zip(bars, [avg_base] + avg_model_counts)):
    ax8.text(i, count + 1, f'{count:.1f}', ha='center', fontweight='bold')
ax8.grid(axis='y', alpha=0.3)

# 9. Line Chart: Job-by-Job Jaccard Similarity
ax9 = plt.subplot(3, 4, 9)
for i, model in enumerate(model_names):
    ax9.plot(stats[model]["jaccard"], label=model, marker='o', 
             linewidth=2, alpha=0.7, color=colors[i])

ax9.set_xlabel('Job Index', fontsize=12)
ax9.set_ylabel('Jaccard Similarity', fontsize=12)
ax9.set_title('Job-by-Job Similarity', fontsize=13, fontweight='bold')
ax9.legend()
ax9.grid(True, alpha=0.3)
ax9.set_ylim(0, 1)

# 10. Heatmap: Metrics Comparison
ax10 = plt.subplot(3, 4, 10)
metrics = ['Jaccard', 'Recall', 'Precision', 'F1']
heatmap_data = []
for model in model_names:
    p = np.mean(stats[model]["precision"])
    r = np.mean(stats[model]["recall"])
    f1 = 2 * (p * r) / (p + r) if (p + r) > 0 else 0
    row = [
        np.mean(stats[model]["jaccard"]),
        r,
        p,
        f1
    ]
    heatmap_data.append(row)

sns.heatmap(heatmap_data, annot=True, fmt='.3f', cmap='RdYlGn', 
            xticklabels=metrics, yticklabels=model_names,
            cbar_kws={'label': 'Score'}, ax=ax10, vmin=0, vmax=1)
ax10.set_title('All Metrics Heatmap', fontsize=13, fontweight='bold')

# 11. Missing Skills Rate
ax11 = plt.subplot(3, 4, 11)
missing_rates = []
for model in model_names:
    total_base = sum(stats[model]["base_totals"])
    total_missing = sum(stats[model]["only_base_counts"])
    missing_rate = total_missing / total_base if total_base > 0 else 0
    missing_rates.append(missing_rate)

bars = ax11.bar(model_names, missing_rates, color=colors, alpha=0.8, edgecolor='black')
ax11.set_ylabel('Missing Rate', fontsize=12)
ax11.set_title('Skills Missing from Model\n(Lower is Better)', fontsize=13, fontweight='bold')
ax11.set_ylim(0, 1)
for i, (bar, val) in enumerate(zip(bars, missing_rates)):
    ax11.text(i, val + 0.02, f'{val:.1%}', ha='center', fontweight='bold')
ax11.grid(axis='y', alpha=0.3)

# 12. Summary Table
ax12 = plt.subplot(3, 4, 12)
ax12.axis('tight')
ax12.axis('off')

table_data = [['Model', 'Jaccard', 'Recall', 'Precision', 'F1', 'Missing%']]
for i, model in enumerate(model_names):
    p = np.mean(stats[model]["precision"])
    r = np.mean(stats[model]["recall"])
    f1 = 2 * (p * r) / (p + r) if (p + r) > 0 else 0
    
    table_data.append([
        model,
        f'{np.mean(stats[model]["jaccard"]):.3f}',
        f'{r:.3f}',
        f'{p:.3f}',
        f'{f1:.3f}',
        f'{missing_rates[i]:.1%}'
    ])

table = ax12.table(cellText=table_data, cellLoc='center', loc='center',
                   colWidths=[0.22, 0.13, 0.13, 0.13, 0.13, 0.13])
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1, 2.5)

# Style header row
for i in range(6):
    table[(0, i)].set_facecolor('#34495E')
    table[(0, i)].set_text_props(weight='bold', color='white')

# Color code rows
for i in range(1, len(model_names) + 1):
    for j in range(6):
        if j == 0:
            table[(i, j)].set_facecolor('#ECF0F1')
        else:
            table[(i, j)].set_facecolor('#FFFFFF')

ax12.set_title('Summary Statistics', fontsize=13, fontweight='bold', pad=20)

plt.suptitle('Skills Overlap Analysis: Base vs Models', fontsize=16, fontweight='bold', y=0.995)
plt.tight_layout()

plt.savefig('/Users/aj/Downloads/DhikrUp/contractJobs/skills_overlap_analysis.png', 
            dpi=300, bbox_inches='tight')
print("Visualization saved to: /Users/aj/Downloads/DhikrUp/contractJobs/skills_overlap_analysis.png")

# Print detailed statistics
print("\n" + "="*90)
print("SKILLS OVERLAP ANALYSIS")
print("="*90)

for model in model_names:
    p = np.mean(stats[model]["precision"])
    r = np.mean(stats[model]["recall"])
    f1 = 2 * (p * r) / (p + r) if (p + r) > 0 else 0
    
    print(f"\n{model.upper()}:")
    print(f"  Jaccard Similarity:     {np.mean(stats[model]['jaccard']):.3f}")
    print(f"  Recall (Coverage):      {r:.3f} ({r:.1%} of base skills found)")
    print(f"  Precision (Accuracy):   {p:.3f} ({p:.1%} of model skills are correct)")
    print(f"  F1 Score:               {f1:.3f}")
    print(f"  Avg Skills in Both:     {np.mean(stats[model]['intersection_counts']):.1f}")
    print(f"  Avg Missing from Model: {np.mean(stats[model]['only_base_counts']):.1f}")
    print(f"  Avg Extra in Model:     {np.mean(stats[model]['only_model_counts']):.1f}")
    print(f"  Missing Rate:           {missing_rates[model_names.index(model)]:.1%}")

print("\n" + "="*90)

# Find jobs with highest missing skills for each model
print("\nJOBS WITH HIGHEST MISSING SKILLS")
print("="*90)

for model in model_names:
    print(f"\n{model.upper()} - Top 5 Jobs with Most Missing Skills:")
    
    # Create list of (job_index, missing_count, missing_skills, description_preview)
    job_missing_data = []
    for result in all_results:
        if model in result["models"]:
            missing_count = result["models"][model]["only_in_base_count"]
            missing_skills = result["models"][model]["only_in_base"]
            job_idx = result["job_index"]
            
            # Get description preview from original data
            desc_preview = data[job_idx]["description"][:150] + "..."
            
            job_missing_data.append({
                "job_index": job_idx,
                "missing_count": missing_count,
                "missing_skills": missing_skills,
                "description": desc_preview,
                "base_total": result["models"][model]["base_total"],
                "missing_rate": missing_count / result["models"][model]["base_total"] if result["models"][model]["base_total"] > 0 else 0
            })
    
    # Sort by missing count (descending)
    job_missing_data.sort(key=lambda x: x["missing_count"], reverse=True)
    
    # Print top 5
    for i, job_data in enumerate(job_missing_data[:5], 1):
        print(f"\n  #{i} - Job Index: {job_data['job_index']}")
        print(f"      Missing Skills: {job_data['missing_count']} out of {job_data['base_total']} ({job_data['missing_rate']:.1%})")
        print(f"      Description: {job_data['description']}")
        print(f"      Missing: {', '.join(list(job_data['missing_skills'])[:10])}")
        if len(job_data['missing_skills']) > 10:
            print(f"               ... and {len(job_data['missing_skills']) - 10} more")

# Save detailed missing skills report to JSON
missing_skills_report = {}
for model in model_names:
    job_missing_data = []
    for result in all_results:
        if model in result["models"]:
            job_missing_data.append({
                "job_index": result["job_index"],
                "missing_count": result["models"][model]["only_in_base_count"],
                "missing_skills": list(result["models"][model]["only_in_base"]),
                "missing_rate": result["models"][model]["only_in_base_count"] / result["models"][model]["base_total"] if result["models"][model]["base_total"] > 0 else 0,
                "base_total": result["models"][model]["base_total"]
            })
    
    job_missing_data.sort(key=lambda x: x["missing_count"], reverse=True)
    missing_skills_report[model] = job_missing_data

report_path = "/Users/aj/Downloads/DhikrUp/contractJobs/missing_skills_report.json"
with open(report_path, "w") as f:
    json.dump(missing_skills_report, f, indent=2)

print(f"\n\nDetailed missing skills report saved to: {report_path}")

# Analyze overlap in top 5 problematic jobs across models
print("\n" + "="*90)
print("TOP 5 PROBLEMATIC JOBS - CROSS-MODEL ANALYSIS")
print("="*90)

# Collect top 5 job indices for each model
top5_by_model = {}
for model in model_names:
    job_missing_data = []
    for result in all_results:
        if model in result["models"]:
            job_missing_data.append({
                "job_index": result["job_index"],
                "missing_count": result["models"][model]["only_in_base_count"],
                "missing_rate": result["models"][model]["only_in_base_count"] / result["models"][model]["base_total"] if result["models"][model]["base_total"] > 0 else 0
            })
    
    job_missing_data.sort(key=lambda x: x["missing_count"], reverse=True)
    top5_by_model[model] = [job["job_index"] for job in job_missing_data[:5]]

# Count how many models each job appears in (within top 5)
job_appearance_count = defaultdict(list)
for model, job_indices in top5_by_model.items():
    for job_idx in job_indices:
        job_appearance_count[job_idx].append(model)

# Sort by appearance count
sorted_jobs = sorted(job_appearance_count.items(), key=lambda x: len(x[1]), reverse=True)

print("\nJobs appearing in multiple models' top 5:")
print("-" * 90)

if sorted_jobs:
    for job_idx, models_list in sorted_jobs:
        appearance_score = len(models_list)
        if appearance_score > 1:  # Only show jobs that appear in more than one model
            print(f"\nJob Index {job_idx} - Appears in {appearance_score} model(s): {', '.join(models_list)}")
            
            # Show description preview
            desc_preview = data[job_idx]["description"][:150] + "..."
            print(f"  Description: {desc_preview}")
            
            # Show missing count for each model it appears in
            for model in models_list:
                for result in all_results:
                    if result["job_index"] == job_idx and model in result["models"]:
                        missing = result["models"][model]["only_in_base_count"]
                        total = result["models"][model]["base_total"]
                        rate = missing / total if total > 0 else 0
                        print(f"    {model}: {missing}/{total} missing ({rate:.1%})")
                        break
else:
    print("No jobs found in analysis.")

# Summary statistics
print("\n" + "-" * 90)
print("SUMMARY:")
appearance_counts = [len(models) for models in job_appearance_count.values()]
if appearance_counts:
    jobs_in_4 = sum(1 for count in appearance_counts if count == 4)
    jobs_in_3 = sum(1 for count in appearance_counts if count == 3)
    jobs_in_2 = sum(1 for count in appearance_counts if count == 2)
    jobs_in_1 = sum(1 for count in appearance_counts if count == 1)
    
    print(f"  Jobs in all 4 models' top 5: {jobs_in_4}")
    print(f"  Jobs in 3 models' top 5: {jobs_in_3}")
    print(f"  Jobs in 2 models' top 5: {jobs_in_2}")
    print(f"  Jobs in only 1 model's top 5: {jobs_in_1}")

# Print each model's top 5 for reference
print("\n" + "="*90)
print("REFERENCE: Each Model's Top 5 Job Indices")
print("="*90)
for model in model_names:
    print(f"{model}: {top5_by_model[model]}")

plt.show()