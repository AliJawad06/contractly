import json
from fastapi import FastAPI, Request, Header, HTTPException
from redis import Redis
from rq import Queue
from worker import process_event

app = FastAPI()

redis_conn = Redis()
queue = Queue("webhook", connection=redis_conn)


@app.post("/webhook")
async def webhook(request: Request):
    payload = await request.json()
    print("webhook reached")
    print(payload.get("type"))
    if payload.get("type") != "batch.completed":
        print("Not right")
        return {"status": "ignored"}

    print(payload.get("type"))
    queue.enqueue(process_event, payload)
    print("response completed recieved")
    return {"status": "ok"}
