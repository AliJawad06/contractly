import json 
import sys
import tiktoken

with open("/Users/aj/Downloads/DhikrUp/contractJobs/tests/combine/combined.json", 'r') as r: 
    jobs = json.load(r)
    titles = [job["title"] for job in jobs]
    u_titles = list(set(titles))
    print(f"len titles: {len(titles)}")
    print(f"unique titles: {len(u_titles)}")
    n= 0
    l = 0
    u_titles.sort()
    strr = ""
    for title in u_titles:
        print(title)
        strr = title + "," + strr
        l = l + len(title)
        encoding = tiktoken.encoding_for_model("gpt-4o")
        n = n + len(encoding.encode(title))
    print(f"tokens: {n}")
    print(f"char len: {l}")
    print(f"joined list tokens: {len(encoding.encode(strr))}")



    


        



