import anthropic
from fastapi import FastAPI, Request, Response, HTTPException
from redis import Redis
from rq import Queue
from worker import process_event

app = FastAPI()
client = anthropic.Anthropic()  # reads ANTHROPIC_WEBHOOK_SIGNING_KEY from env

redis_conn = Redis()
queue = Queue("webhook", connection=redis_conn)

# In-memory dedupe — replace with Redis in production for multi-process safety
seen_event_ids: set[str] = set()


@app.post("/webhook")
async def webhook(request: Request):
    # Must use raw body — signature is computed over raw bytes, not parsed JSON
    body = (await request.body()).decode("utf-8")
    headers = dict(request.headers)

    try:
        event = client.beta.webhooks.unwrap(body, headers=headers)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid signature")

    # event.id is per-event, not per-delivery — discard retries
    if event.id in seen_event_ids:
        return Response(status_code=204)
    seen_event_ids.add(event.id)

    match event.data.type:
        case "session.status_idled":
            print("session idled:", event.data.id)
            queue.enqueue(process_event, {"data": {"id": event.data.id}})
        case "session.status_terminated":
            print("session terminated:", event.data.id)
        case "session.status_run_started":
            print("session run started:", event.data.id)
        case "vault_credential.refresh_failed":
            print("vault credential refresh failed:", event.data.id)
        case _:
            print(f"unhandled event type: {event.data.type}", event.data.id)

    return Response(status_code=204)
