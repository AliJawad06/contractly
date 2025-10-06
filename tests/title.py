import json
import random
from collections import Counter

titles = []
with open('/Users/aj/Downloads/DhikrUp/contractJobs/all_companies_job_details_last_15_days_20251003_175606.json', 'r') as f:
    data = json.load(f)
    jobs = data['jobs']
    for job in jobs:
        title = job['title']
        titles.append(title)

# Count occurrences
n = Counter(titles)
jtitles = []
for title in n:
    jtitles.append(title)

with open ('titles.json','w') as w:
    json.dump(jtitles,w,indent=2)
    


# Sort by frequency (descending)
#sorted_counted = dict(sorted(n.items(), key=lambda x: x[1], reverse=True))

# 🎲 Pick 50 random (item, count) pairs
#sampled = random.sample(list(sorted_counted.items()), 1)

# Print them
#for item, count in sampled:
#   print(f"{item}: {count}")

tcount = 0
for item, count in list(sorted_counted.items())[:8614]:
    print(f"{item}, {count}")
    tcount = count + tcount

print(tcount)