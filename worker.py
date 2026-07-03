import json
import re
from pathlib import Path
import anthropic
from redis import Redis
from rq import Worker
import sys
from pathlib import Path
import time 

OUTPUT_PATH = Path("/root/contractly/day/day.jsonl")


def load_existing_urls():
    seen = set()
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH) as f:
            for line in f:
                if line.strip():
                    try:
                        seen.add(json.loads(line)["url"])
                    except:
                        pass
    return seen


def process_event():
    client = anthropic.Anthropic()

    OUTPUT_PATH = Path("/root/contractly/day/day.jsonl")

    with open("/root/contractly/day/day_loc_processing.jsonl") as f:
        jobs = [json.loads(line) for line in f if line.strip()]

    lookup = {obj["custom_id"]: obj for obj in jobs if obj.get("custom_id")}
    batch_id = Path("/root/contractly/pending_batch.txt").read_text().strip()
    batch = client.messages.batches.retrieve(batch_id)
    Path("/root/contractly/pending_batch.txt").unlink()
    print(f"batch{batch_id} is being processed")
    while batch.processing_status != "ended":
         print({"status": "not_ready"})
         batch = client.messages.batches.retrieve(batch_id)
         time.sleep(300)

    seen = load_existing_urls()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    written = 0

    with open(OUTPUT_PATH, "a") as f:
        for result in client.messages.batches.results(batch_id):
            try:
                if result.result.type != "succeeded":
                    print(f"Skipping {result.custom_id}: {result.result.type}")
                else:
                    output = result.result.message.content[0].text
                    parsed = json.loads(output) 
                    if parsed.get("yoe") is not None:
                        parsed["experience"] = parsed.pop("yoe")
                    if parsed.get("locations"):
                        locs = [f"{l['city']}, {l['state']}" for l in parsed.get("locations")]
                        parsed["location"] = parsed.get("locations")
                        parsed.pop("locations")
                    match = lookup.get(result.custom_id)
                    if not match:
                        continue
                    job_url = match.get("url", "")
                    if job_url in seen:
                        continue
                    print(result.custom_id, match["experience"], parsed.get("experience") or "no parse")
                    merged = {**match, **parsed}
                    f.write(json.dumps(merged) + "\n")
                    seen.add(job_url)
                    written += 1
            except Exception as e:
                print(f"Error processing result {result.custom_id}: {e}")
                continue

    return {"status": "processed", "written": written}



if __name__ == "__main__":
    #payload = {}
    #payload["data"] = {"id": "msgbatch_013waLQf7kvRFGaLqEFeXeRJ"}
    process_event()
    #worker = Worker(["webhook"], connection=redis_conn)
    #worker.work()
