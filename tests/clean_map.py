import json 

new_data = {}

with open("/Users/aj/Downloads/DhikrUp/contractJobs/location_mapper.json", 'r') as r, open("newmap.json", 'w') as w: 
    data = json.load(r)
    for loc in data:
        data[loc] = [data[loc]]

    json.dump(data, w, indent=2)
    