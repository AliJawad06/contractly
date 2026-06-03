import json
from redis import Redis
from rq import Worker
from tests.users.job_matcher import new_user

def process_event(payload):
    email = payload["email"]
    try:
        new_user(email)

    except Exception as e:
        print(f"failed to send email to {email}")
        print(f"Error: {e}")
        return {"status": "error"}

    return {"status": "processed", "written": email}



redis_conn = Redis()

if __name__ == "__main__":
    worker = Worker(["profile_webhook"], connection = redis_conn)
    worker.work()
