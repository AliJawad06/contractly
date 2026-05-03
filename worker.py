import json
from pathlib import Path
from openai import OpenAI
from redis import Redis
from rq import Worker

client = OpenAI()

OUTPUT_PATH = Path("/root/contractly/day/day.jsonl")

with open("/root/contractly/day/day_intermediate.jsonl") as f:
    jobs = [json.loads(line) for line in f if line.strip()]

lookup = {obj["url"]: obj for obj in jobs}


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


def extract_text(obj):
    body = obj["response"]["body"]

    # Responses API format
    if "output" in body:
        for block in body["output"]:
            if block.get("type") == "message":
                for part in block.get("content", []):
                    if part.get("type") == "output_text":
                        return part["text"]
            # flat content block
            if "content" in block:
                for part in block["content"]:
                    if "text" in part:
                        return part["text"]

    # Chat Completions format
    if "choices" in body:
        return body["choices"][0]["message"]["content"]

    raise ValueError(f"Unknown response structure. Keys: {list(body.keys())}")


def process_event(payload):
    batch_id = payload["data"]["id"]
    batch = client.batches.retrieve(batch_id)
    print("event is being processed")
    if not batch.output_file_id:
        return {"status": "no_output"}
    print(payload)
    file_response = client.files.content(batch.output_file_id)
    seen = load_existing_urls()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    written = 0

    # Debug: print the first line's structure then remove this block
    print(file_response.text)
    for line in file_response.text.splitlines():
        
        if line.strip():
            obj = json.loads(line)
            print("=== FIRST LINE STRUCTURE ===")
            print(json.dumps(obj["response"]["body"], indent=2, default=str))
            break

    with open(OUTPUT_PATH, "a") as f:
        for line in file_response.text.splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                text_json = extract_text(obj)
                parsed = json.loads(text_json)
                url = obj["custom_id"]
                match = lookup.get(url)
                if not match or url in seen:
                    continue
                merged = {**match, **parsed}
                f.write(json.dumps(merged) + "\n")
                seen.add(url)
                written += 1
            except Exception as e:
                print(f"Error processing line: {e}")
                continue

    return {"status": "processed", "written": written}


redis_conn = Redis()

if __name__ == "__main__":
    worker = Worker(["webhook"], connection=redis_conn)
    worker.work()
