import anthropic
from fastapi import FastAPI, Request, Response, HTTPException
from redis import Redis
from rq import Queue
from profile_worker import process_event

app = FastAPI()
client = anthropic.Anthropic()  # reads ANTHROPIC_WEBHOOK_SIGNING_KEY from env

redis_conn = Redis()
queue = Queue("profile_webhook", connection=redis_conn)

# In-memory dedupe — replace with Redis in production for multi-process safety
seen_event_ids: set[str] = set()


@app.post("/profile_webhook")
async def webhook(request: Request):
    print("Webhook_reached")
    # Must use raw body — signature is computed over raw bytes, not parsed JSON
    body = await request.json()
    print(body)
    email = body["record"]["email"]
    if email:
        queue.enqueue(process_event,{"email": email})

    return Response(status_code=204)
                                                                                                                                            
