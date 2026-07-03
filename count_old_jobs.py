#!/usr/bin/env python3
"""
Scan a cron.log file for lines reporting old jobs in 'x/y old jobs' form,
then sum all x (old jobs) and all y-x (new/non-old jobs) across those lines.

Usage:
    python3 count_old_jobs.py [path_to_log]   # defaults to cron.log
"""
import re
import sys

# Matches the 'x/y old jobs' portion, e.g. "4/20 old jobs"
PATTERN = re.compile(r"(\d+)\s*/\s*(\d+)\s+old jobs")


def main(path: str) -> None:
    sum_x = 0          # total old jobs (numerator)
    sum_y_minus_x = 0  # total non-old jobs (denominator - numerator)
    sum_y = 0          # total jobs seen (denominator)
    matched_lines = 0
    skipped = 0

    try:
        with open(path) as f:
            for line in f:
                if "old jobs" not in line:
                    continue
                m = PATTERN.search(line)
                if not m:
                    skipped += 1
                    print(f"  could not parse x/y in: {line.strip()}")
                    continue
                x, y = int(m.group(1)), int(m.group(2))
                sum_x += x
                sum_y += y
                sum_y_minus_x += (y - x)
                matched_lines += 1
    except FileNotFoundError:
        print(f"File not found: {path}")
        sys.exit(1)

    print(f"Lines with 'old jobs':        {matched_lines}")
    if skipped:
        print(f"Lines skipped (no x/y):       {skipped}")
    print(f"Sum of x  (old jobs):         {sum_x}")
    print(f"Sum of y-x (new jobs):        {sum_y_minus_x}")
    print(f"Sum of y  (total, = x + y-x): {sum_y}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "cron.log"
    main(path)
