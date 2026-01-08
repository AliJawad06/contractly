import json 

import re 

with open("/Users/aj/Downloads/DhikrUp/contractJobs/all_combined.json", "r") as outfile:
    jobs = json.load(outfile)

    pattern = r'^[A-Z]{2}, {1,2}[A-Za-z]+$'
    pattern2 = r'^[A-Z]{2}, [A-Za-z]+ [A-Za-z]+$'


    locations = [job["location"] for job in jobs ]

    ulocations = list(set(locations))

    p1 = 0
    p2 = 0
    l = 0 
    for loc in ulocations:
        if re.match(pattern, loc):
            p1 = p1 + 1
        elif re.match(pattern2, loc):
            p2 = p2+1
        else: 
            print(f'"{loc}",')
