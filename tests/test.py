import json
from openai import OpenAI


if __name__ == "__main__":
    with open("/root/contractly/day/day.jsonl", "r") as r:
        i = 0
        for job in r:
            i = i + 1
            job = json.loads(job)
            print(i, job.get("start"), type(job.get("start").get("start_dateStr")))
