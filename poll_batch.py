#!/usr/bin/env python3
"""
Run via cron every 5 minutes:
  */5 * * * * /usr/bin/python3 /root/contractly/poll_batch.py >> /root/contractly/poll.log 2>&1
"""
import sys
from pathlib import Path
import anthropic
from worker import process_event

PENDING = Path("/root/contractly/pending_batch.txt")


def main():
    if not PENDING.exists():
        print("no pending batch")
        return

    batch_id = PENDING.read_text().strip()
    if not batch_id:
        return

    client = anthropic.Anthropic()
    batch = client.messages.batches.retrieve(batch_id)
    print(f"batch {batch_id}: {batch.processing_status}")

    if batch.processing_status != "ended":
        return

    result = process_event({"data": {"id": batch_id}})
    print(f"processed: {result}")
    PENDING.unlink()


if __name__ == "__main__":
    main()
