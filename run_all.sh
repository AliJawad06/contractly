#!/bin/bash

# Run Python scripts in order
# Each script waits for the previous one to complete
# > day_loc_processing.jsonl
# > batching_tests.jsonl
echo "Starting batch processing..."
echo "================================"


cd /root/contractly
source venv/bin/activate

set -a
source /root/contractly/.env
set +a

SCRIPTS=(
	"source venv/bin/activate"
	"> day/day.jsonl"
        "> day/day_intermediate.jsonl"
        "> day/day_loc_processing.jsonl"
	"> batching_tests.jsonl"
	"python main.py"
	"python jobapi.py"
	"python combine.py"
        "python worker.py"
	"python tests/users/job_matcher.py"
)



# Method 4: With timing
start_time=$(date +%s)

for script in "${SCRIPTS[@]}"; do
    script_start=$(date +%s)
    echo "[$(date '+%H:%M:%S')] Running $script..."
    
    eval "$script"
    
    script_end=$(date +%s)
    echo "$script completed in $((script_end - script_start)) seconds"
done

end_time=$(date +%s)
echo "================================"
echo "Total time: $((end_time - start_time)) seconds"

