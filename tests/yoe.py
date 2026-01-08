import json
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from collections import defaultdict, Counter

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (22, 14)

# Load data
file_path = "/Users/aj/Downloads/DhikrUp/contractJobs/4.1msample.json"

with open(file_path, "r") as r:
    data = json.load(r)

model_names = ["five", "mini", "nano", "nano_alt", "mini_alt", "4.1", "4.1m"]

# Extract YoE for each job and model
yoe_data = []

for idx, job in enumerate(data):
    try:
        base_data = json.loads(job["base"])
        base_yoe = base_data.get("calendar_event", {}).get("yoE", None)
        
        job_yoe = {
            "job_index": idx,
            "base_yoe": base_yoe,
            "description_preview": job["description"][:100] + "...",
            "models": {}
        }
        
        for model_name in model_names:
            if model_name in job:
                model_data = json.loads(job[model_name])
                model_yoe = model_data.get("calendar_event", {}).get("yoE", None)
                
                # Calculate error
                exact_match = (base_yoe == model_yoe)
                error = abs(base_yoe - model_yoe) if (base_yoe is not None and model_yoe is not None) else None
                
                job_yoe["models"][model_name] = {
                    "yoe": model_yoe,
                    "exact_match": exact_match,
                    "error": error
                }
        
        yoe_data.append(job_yoe)
        
    except Exception as e:
        print(f"Error processing job {idx}: {e}")

print(f"Processed {len(yoe_data)} jobs")

# Calculate statistics
stats = {model: {
    "exact_matches": 0,
    "total_compared": 0,
    "errors": [],
    "base_vs_model": [],  # (base, model) pairs
    "mae": 0,  # Mean Absolute Error
    "rmse": 0,  # Root Mean Squared Error
} for model in model_names}

for job in yoe_data:
    base_yoe = job["base_yoe"]
    
    for model in model_names:
        if model in job["models"]:
            model_info = job["models"][model]
            
            if model_info["exact_match"] is not None:
                stats[model]["total_compared"] += 1
                
                if model_info["exact_match"]:
                    stats[model]["exact_matches"] += 1
                
                if model_info["error"] is not None:
                    stats[model]["errors"].append(model_info["error"])
                    stats[model]["base_vs_model"].append((base_yoe, model_info["yoe"]))

# Calculate MAE and RMSE
for model in model_names:
    errors = stats[model]["errors"]
    if errors:
        stats[model]["mae"] = np.mean(errors)
        stats[model]["rmse"] = np.sqrt(np.mean([e**2 for e in errors]))
        stats[model]["accuracy"] = stats[model]["exact_matches"] / stats[model]["total_compared"]
    else:
        stats[model]["accuracy"] = 0

# Create visualizations
fig = plt.figure(figsize=(22, 14))
colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A', '#9B59B6', '#E67E22', "#0004FB"]

# 1. Accuracy (Exact Match Rate)
ax1 = plt.subplot(3, 4, 1)
accuracies = [stats[model]["accuracy"] for model in model_names]
bars = ax1.bar(model_names, accuracies, color=colors, alpha=0.8, edgecolor='black')
ax1.set_ylabel('Accuracy', fontsize=12)
ax1.set_title('YoE Exact Match Accuracy', fontsize=14, fontweight='bold')
ax1.set_ylim(0, 1)
ax1.set_xticklabels(model_names, rotation=45, ha='right')
for i, (bar, acc) in enumerate(zip(bars, accuracies)):
    ax1.text(i, acc + 0.02, f'{acc:.1%}', ha='center', fontweight='bold')
ax1.grid(axis='y', alpha=0.3)

# 2. Mean Absolute Error (MAE)
ax2 = plt.subplot(3, 4, 2)
maes = [stats[model]["mae"] for model in model_names]
bars = ax2.bar(model_names, maes, color=colors, alpha=0.8, edgecolor='black')
ax2.set_ylabel('MAE (years)', fontsize=12)
ax2.set_title('Mean Absolute Error', fontsize=14, fontweight='bold')
ax2.set_xticklabels(model_names, rotation=45, ha='right')
for i, (bar, mae) in enumerate(zip(bars, maes)):
    ax2.text(i, mae + 0.05, f'{mae:.2f}', ha='center', fontweight='bold')
ax2.grid(axis='y', alpha=0.3)

# 3. RMSE
ax3 = plt.subplot(3, 4, 3)
rmses = [stats[model]["rmse"] for model in model_names]
bars = ax3.bar(model_names, rmses, color=colors, alpha=0.8, edgecolor='black')
ax3.set_ylabel('RMSE (years)', fontsize=12)
ax3.set_title('Root Mean Squared Error', fontsize=14, fontweight='bold')
ax3.set_xticklabels(model_names, rotation=45, ha='right')
for i, (bar, rmse) in enumerate(zip(bars, rmses)):
    ax3.text(i, rmse + 0.05, f'{rmse:.2f}', ha='center', fontweight='bold')
ax3.grid(axis='y', alpha=0.3)

# 4. Error Distribution (Box Plot)
ax4 = plt.subplot(3, 4, 4)
error_data = [stats[model]["errors"] for model in model_names]
bp = ax4.boxplot(error_data, labels=model_names, patch_artist=True)
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)
ax4.set_ylabel('Absolute Error (years)', fontsize=12)
ax4.set_title('Error Distribution', fontsize=14, fontweight='bold')
ax4.set_xticklabels(model_names, rotation=45, ha='right')
ax4.grid(axis='y', alpha=0.3)

# 5. Error Histogram for each model
for i, model in enumerate(model_names):
    ax = plt.subplot(3, 4, 5 + i)
    errors = stats[model]["errors"]
    if errors:
        ax.hist(errors, bins=range(0, max(errors)+2), alpha=0.7, 
                color=colors[i], edgecolor='black')
        ax.axvline(stats[model]["mae"], color='red', linestyle='--', 
                   linewidth=2, label=f'MAE: {stats[model]["mae"]:.2f}')
    ax.set_xlabel('Error (years)', fontsize=10)
    ax.set_ylabel('Frequency', fontsize=10)
    ax.set_title(f'{model}', fontsize=12, fontweight='bold')
    ax.legend(fontsize=9)
    ax.grid(axis='y', alpha=0.3)

# 11. Confusion Matrix Style - Base vs Model predictions
ax11 = plt.subplot(3, 4, 11)
all_base_yoe = [job["base_yoe"] for job in yoe_data if job["base_yoe"] is not None]
unique_yoe = sorted(set(all_base_yoe))

# Create matrix for first model (you can change this)
model_to_show = "nano_alt"
confusion_data = defaultdict(lambda: defaultdict(int))

for job in yoe_data:
    if job["base_yoe"] is not None and model_to_show in job["models"]:
        base = job["base_yoe"]
        model_pred = job["models"][model_to_show]["yoe"]
        if model_pred is not None:
            confusion_data[base][model_pred] += 1

# Convert to matrix
matrix = []
for base_val in unique_yoe:
    row = [confusion_data[base_val][model_val] for model_val in unique_yoe]
    matrix.append(row)

sns.heatmap(matrix, annot=True, fmt='d', cmap='YlOrRd', 
            xticklabels=unique_yoe, yticklabels=unique_yoe,
            cbar_kws={'label': 'Count'}, ax=ax11)
ax11.set_xlabel('Predicted YoE', fontsize=12)
ax11.set_ylabel('Actual YoE', fontsize=12)
ax11.set_title(f'Confusion Matrix: {model_to_show}', fontsize=14, fontweight='bold')

# 12. Summary Statistics Table
ax12 = plt.subplot(3, 4, 12)
ax12.axis('tight')
ax12.axis('off')

table_data = [['Model', 'Accuracy', 'MAE', 'RMSE', 'Matches', 'Total']]
for model in model_names:
    table_data.append([
        model,
        f'{stats[model]["accuracy"]:.1%}',
        f'{stats[model]["mae"]:.2f}',
        f'{stats[model]["rmse"]:.2f}',
        f'{stats[model]["exact_matches"]}',
        f'{stats[model]["total_compared"]}'
    ])

table = ax12.table(cellText=table_data, cellLoc='center', loc='center',
                   colWidths=[0.22, 0.13, 0.13, 0.13, 0.13, 0.13])
table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1, 2)

for i in range(6):
    table[(0, i)].set_facecolor('#34495E')
    table[(0, i)].set_text_props(weight='bold', color='white')

# Highlight best accuracy
best_acc_idx = accuracies.index(max(accuracies)) + 1
for j in range(6):
    table[(best_acc_idx, j)].set_facecolor('#D5F4E6')

ax12.set_title('Summary Statistics', fontsize=14, fontweight='bold', pad=20)

plt.suptitle('Years of Experience (YoE) Accuracy Analysis', 
             fontsize=16, fontweight='bold', y=0.995)
plt.tight_layout()

plt.savefig('/Users/aj/Downloads/DhikrUp/contractJobs/yoe_accuracy_analysis.png', 
            dpi=300, bbox_inches='tight')
print("Visualization saved to: /Users/aj/Downloads/DhikrUp/contractJobs/yoe_accuracy_analysis.png")

# Print detailed statistics
print("\n" + "="*90)
print("YEARS OF EXPERIENCE (YoE) ACCURACY ANALYSIS")
print("="*90)

for model in model_names:
    print(f"\n{model.upper()}:")
    print(f"  Accuracy (Exact Match): {stats[model]['accuracy']:.1%}")
    print(f"  Exact Matches: {stats[model]['exact_matches']}/{stats[model]['total_compared']}")
    print(f"  Mean Absolute Error: {stats[model]['mae']:.2f} years")
    print(f"  Root Mean Squared Error: {stats[model]['rmse']:.2f} years")
    
    if stats[model]["errors"]:
        error_counter = Counter(stats[model]["errors"])
        print(f"  Error breakdown:")
        for error, count in sorted(error_counter.items()):
            print(f"    Off by {error} year(s): {count} jobs")

# Find worst predictions for each model
print("\n" + "="*90)
print("WORST PREDICTIONS (Top 3 per model)")
print("="*90)

for model in model_names:
    print(f"\n{model.upper()}:")
    
    model_worst = []
    for job in yoe_data:
        if model in job["models"] and job["models"][model]["error"] is not None:
            model_worst.append({
                "job_index": job["job_index"],
                "base_yoe": job["base_yoe"],
                "model_yoe": job["models"][model]["yoe"],
                "error": job["models"][model]["error"],
                "description": job["description_preview"]
            })
    
    model_worst.sort(key=lambda x: x["error"], reverse=True)
    
    for i, worst in enumerate(model_worst[:3], 1):
        print(f"  #{i} Job {worst['job_index']}: Base={worst['base_yoe']}, "
              f"Predicted={worst['model_yoe']}, Error={worst['error']}")
        print(f"      {worst['description']}")

# Base YoE distribution
print("\n" + "="*90)
print("BASE YoE DISTRIBUTION")
print("="*90)

base_yoe_counts = Counter([job["base_yoe"] for job in yoe_data if job["base_yoe"] is not None])
for yoe, count in sorted(base_yoe_counts.items()):
    print(f"  {yoe} years: {count} jobs ({count/len(yoe_data)*100:.1f}%)")

# Save detailed report
report = {
    "summary": {model: {
        "accuracy": float(stats[model]["accuracy"]),
        "exact_matches": stats[model]["exact_matches"],
        "total_compared": stats[model]["total_compared"],
        "mae": float(stats[model]["mae"]),
        "rmse": float(stats[model]["rmse"])
    } for model in model_names},
    "base_distribution": dict(base_yoe_counts),
    "detailed_predictions": yoe_data
}

report_path = "/Users/aj/Downloads/DhikrUp/contractJobs/yoe_accuracy_report.json"
with open(report_path, "w") as f:
    json.dump(report, f, indent=2)

print(f"\n\nDetailed report saved to: {report_path}")

plt.show()