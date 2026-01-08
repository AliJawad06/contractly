import json
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Set
from sklearn.model_selection import train_test_split

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (18, 10)

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

def calculate_error_metrics(base_skills: Set[str], model_skills: Set[str]) -> Dict:
    """Calculate error metrics between base and model."""
    intersection = base_skills & model_skills
    only_in_base = base_skills - model_skills
    only_in_model = model_skills - base_skills
    
    total_unique = len(base_skills | model_skills)
    
    jaccard = len(intersection) / total_unique if total_unique > 0 else 0
    recall = len(intersection) / len(base_skills) if len(base_skills) > 0 else 0
    precision = len(intersection) / len(model_skills) if len(model_skills) > 0 else 0
    
    # Error is 1 - jaccard (higher error = worse performance)
    error = 1 - jaccard
    
    return {
        "jaccard": jaccard,
        "recall": recall,
        "precision": precision,
        "error": error,
        "missing_count": len(only_in_base),
        "extra_count": len(only_in_model)
    }

# Load data
file_path = "/Users/aj/Downloads/DhikrUp/contractJobs/compare_all_output.json"

with open(file_path, "r") as r:
    data = json.load(r)

print(f"Total samples: {len(data)}")

# Calculate nano_alt error for each sample
job_errors = []

for idx, job in enumerate(data):
    try:
        base_data = json.loads(job["base"])
        base_skills = extract_all_skills(base_data)
        
        if "nano_alt" in job:
            nano_alt_data = json.loads(job["nano_alt"])
            nano_alt_skills = extract_all_skills(nano_alt_data)
            
            metrics = calculate_error_metrics(base_skills, nano_alt_skills)
            
            job_errors.append({
                "job_index": idx,
                "error": metrics["error"],
                "jaccard": metrics["jaccard"],
                "recall": metrics["recall"],
                "precision": metrics["precision"],
                "missing_count": metrics["missing_count"],
                "extra_count": metrics["extra_count"],
                "base_skill_count": len(base_skills)
            })
        else:
            print(f"Warning: Job {idx} missing nano_alt data")
            
    except Exception as e:
        print(f"Error processing job {idx}: {e}")

print(f"Successfully processed: {len(job_errors)} jobs")

# Convert to arrays for stratification
indices = np.array([job["job_index"] for job in job_errors])
errors = np.array([job["error"] for job in job_errors])

# Create stratification bins based on error quartiles
# This ensures similar error distribution in train and test
n_bins = 4
error_bins = np.digitize(errors, bins=np.percentile(errors, [25, 50, 75]))

print(f"\nError distribution across bins:")
for i in range(n_bins):
    count = np.sum(error_bins == i)
    print(f"  Bin {i}: {count} samples")

# Stratified split
train_indices, test_indices = train_test_split(
    indices,
    test_size=0.5,  # 50-50 split
    stratify=error_bins,
    random_state=42
)

print(f"\nTrain set size: {len(train_indices)}")
print(f"Test set size: {len(test_indices)}")

# Get error values for train and test sets
train_errors = [job_errors[idx]["error"] for idx in train_indices]
test_errors = [job_errors[idx]["error"] for idx in test_indices]

# Create train and test datasets
train_data = [{str((idx)):data[idx]["nano_alt"],"description":data[idx]["description"]} for idx in train_indices]
test_data = [{str(idx):data[idx]["nano_alt"],"description":data[idx]["description"] } for idx in test_indices]




# Save splits
train_path = "/Users/aj/Downloads/DhikrUp/contractJobs/train_split.json"
test_path = "/Users/aj/Downloads/DhikrUp/contractJobs/test_split.json"

with open(train_path, "w") as f:
    json.dump(train_data, f, indent=2)

with open(test_path, "w") as f:
    json.dump(test_data, f, indent=2)

# Save index mappings
split_info = {
    "train_indices": train_indices.tolist(),
    "test_indices": test_indices.tolist(),
    "train_size": len(train_indices),
    "test_size": len(test_indices),
    "stratification_bins": n_bins
}

info_path = "/Users/aj/Downloads/DhikrUp/contractJobs/split_info.json"
with open(info_path, "w") as f:
    json.dump(split_info, f, indent=2)

print(f"\nTrain data saved to: {train_path}")
print(f"Test data saved to: {test_path}")
print(f"Split info saved to: {info_path}")

# Visualizations
fig = plt.figure(figsize=(18, 10))

# 1. Error Distribution Comparison
ax1 = plt.subplot(2, 3, 1)
ax1.hist(train_errors, bins=20, alpha=0.6, label='Train', color='#3498DB', edgecolor='black')
ax1.hist(test_errors, bins=20, alpha=0.6, label='Test', color='#E74C3C', edgecolor='black')
ax1.set_xlabel('Error (1 - Jaccard Similarity)', fontsize=12)
ax1.set_ylabel('Frequency', fontsize=12)
ax1.set_title('Error Distribution: Train vs Test', fontsize=14, fontweight='bold')
ax1.legend()
ax1.grid(axis='y', alpha=0.3)

# 2. Box Plot Comparison
ax2 = plt.subplot(2, 3, 2)
box_data = [train_errors, test_errors]
bp = ax2.boxplot(box_data, labels=['Train', 'Test'], patch_artist=True)
bp['boxes'][0].set_facecolor('#3498DB')
bp['boxes'][1].set_facecolor('#E74C3C')
ax2.set_ylabel('Error', fontsize=12)
ax2.set_title('Error Distribution Box Plot', fontsize=14, fontweight='bold')
ax2.grid(axis='y', alpha=0.3)

# 3. Cumulative Distribution
ax3 = plt.subplot(2, 3, 3)
train_sorted = np.sort(train_errors)
test_sorted = np.sort(test_errors)
ax3.plot(train_sorted, np.linspace(0, 1, len(train_sorted)), 
         label='Train', linewidth=2, color='#3498DB')
ax3.plot(test_sorted, np.linspace(0, 1, len(test_sorted)), 
         label='Test', linewidth=2, color='#E74C3C', linestyle='--')
ax3.set_xlabel('Error', fontsize=12)
ax3.set_ylabel('Cumulative Probability', fontsize=12)
ax3.set_title('Cumulative Distribution Function', fontsize=14, fontweight='bold')
ax3.legend()
ax3.grid(True, alpha=0.3)

# 4. Statistical Summary
ax4 = plt.subplot(2, 3, 4)
ax4.axis('tight')
ax4.axis('off')

stats_data = [
    ['Metric', 'Train', 'Test', 'Difference'],
    ['Mean Error', f'{np.mean(train_errors):.4f}', f'{np.mean(test_errors):.4f}', 
     f'{abs(np.mean(train_errors) - np.mean(test_errors)):.4f}'],
    ['Median Error', f'{np.median(train_errors):.4f}', f'{np.median(test_errors):.4f}',
     f'{abs(np.median(train_errors) - np.median(test_errors)):.4f}'],
    ['Std Dev', f'{np.std(train_errors):.4f}', f'{np.std(test_errors):.4f}',
     f'{abs(np.std(train_errors) - np.std(test_errors)):.4f}'],
    ['Min Error', f'{np.min(train_errors):.4f}', f'{np.min(test_errors):.4f}',
     f'{abs(np.min(train_errors) - np.min(test_errors)):.4f}'],
    ['Max Error', f'{np.max(train_errors):.4f}', f'{np.max(test_errors):.4f}',
     f'{abs(np.max(train_errors) - np.max(test_errors)):.4f}'],
]

table = ax4.table(cellText=stats_data, cellLoc='center', loc='center',
                  colWidths=[0.25, 0.25, 0.25, 0.25])
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1, 2)

for i in range(4):
    table[(0, i)].set_facecolor('#34495E')
    table[(0, i)].set_text_props(weight='bold', color='white')

ax4.set_title('Statistical Comparison', fontsize=14, fontweight='bold', pad=20)

# 5. Scatter Plot: Train vs Test Errors by Index
ax5 = plt.subplot(2, 3, 5)
ax5.scatter(range(len(train_errors)), sorted(train_errors), 
           alpha=0.6, s=50, label='Train', color='#3498DB')
ax5.scatter(range(len(test_errors)), sorted(test_errors), 
           alpha=0.6, s=50, label='Test', color='#E74C3C')
ax5.set_xlabel('Sample Index (sorted by error)', fontsize=12)
ax5.set_ylabel('Error', fontsize=12)
ax5.set_title('Sorted Error Values', fontsize=14, fontweight='bold')
ax5.legend()
ax5.grid(True, alpha=0.3)

# 6. Quartile Distribution
ax6 = plt.subplot(2, 3, 6)
train_bins = np.digitize([job_errors[idx]["error"] for idx in train_indices], 
                         bins=np.percentile(errors, [25, 50, 75]))
test_bins = np.digitize([job_errors[idx]["error"] for idx in test_indices], 
                        bins=np.percentile(errors, [25, 50, 75]))

x = np.arange(n_bins)
width = 0.35

train_counts = [np.sum(train_bins == i) for i in range(n_bins)]
test_counts = [np.sum(test_bins == i) for i in range(n_bins)]

bars1 = ax6.bar(x - width/2, train_counts, width, label='Train', 
                alpha=0.8, color='#3498DB', edgecolor='black')
bars2 = ax6.bar(x + width/2, test_counts, width, label='Test', 
                alpha=0.8, color='#E74C3C', edgecolor='black')

ax6.set_xlabel('Error Quartile', fontsize=12)
ax6.set_ylabel('Count', fontsize=12)
ax6.set_title('Stratification: Samples per Quartile', fontsize=14, fontweight='bold')
ax6.set_xticks(x)
ax6.set_xticklabels(['Q1 (Low)', 'Q2', 'Q3', 'Q4 (High)'])
ax6.legend()
ax6.grid(axis='y', alpha=0.3)

for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax6.text(bar.get_x() + bar.get_width()/2., height + 0.3,
                f'{int(height)}', ha='center', va='bottom', fontsize=9)

plt.suptitle('Stratified Train-Test Split Analysis (nano_alt error distribution)', 
             fontsize=16, fontweight='bold', y=0.995)
plt.tight_layout()

plt.savefig('/Users/aj/Downloads/DhikrUp/contractJobs/train_test_split_visualization.png', 
            dpi=300, bbox_inches='tight')
print(f"\nVisualization saved to: /Users/aj/Downloads/DhikrUp/contractJobs/train_test_split_visualization.png")

# Print detailed statistics
print("\n" + "="*80)
print("TRAIN-TEST SPLIT STATISTICS")
print("="*80)
print(f"\nTrain Set ({len(train_indices)} samples):")
print(f"  Mean Error: {np.mean(train_errors):.4f}")
print(f"  Median Error: {np.median(train_errors):.4f}")
print(f"  Std Dev: {np.std(train_errors):.4f}")
print(f"  Range: [{np.min(train_errors):.4f}, {np.max(train_errors):.4f}]")

print(f"\nTest Set ({len(test_indices)} samples):")
print(f"  Mean Error: {np.mean(test_errors):.4f}")
print(f"  Median Error: {np.median(test_errors):.4f}")
print(f"  Std Dev: {np.std(test_errors):.4f}")
print(f"  Range: [{np.min(test_errors):.4f}, {np.max(test_errors):.4f}]")

print(f"\nDifferences:")
print(f"  Mean difference: {abs(np.mean(train_errors) - np.mean(test_errors)):.4f}")
print(f"  Median difference: {abs(np.median(train_errors) - np.median(test_errors)):.4f}")
print(f"  Std Dev difference: {abs(np.std(train_errors) - np.std(test_errors)):.4f}")

# Kolmogorov-Smirnov test
from scipy import stats
ks_statistic, p_value = stats.ks_2samp(train_errors, test_errors)
print(f"\nKolmogorov-Smirnov Test:")
print(f"  Statistic: {ks_statistic:.4f}")
print(f"  P-value: {p_value:.4f}")
print(f"  Similar distributions: {'Yes' if p_value > 0.05 else 'No'} (p > 0.05)")

plt.show()