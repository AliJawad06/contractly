import json
from openai import OpenAI


if __name__ == "__main__":
    with open("/Users/aj/Downloads/batch_results_msgbatch_01DUAR8NGgNYMYGtabnb9LoR.jsonl", "r") as r:
        i = 0
        for job in r:
            i = i + 1
            job = json.loads(job)
            if type(job["location"]) is not list:
                print(i, type(job["location"]), job["location"])
                print(job)
