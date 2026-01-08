#!/bin/bash

# Run Python scripts in order
# Each script waits for the previous one to complete

echo "Starting batch processing..."
echo "================================"


SCRIPTS=(
    "/Users/aj/Downloads/DhikrUp/contractJobs/main.py"
    "/Users/aj/Downloads/DhikrUp/contractJobs/jobapi.py"
    "/Users/aj/Downloads/DhikrUp/contractJobs/tests/combine.py"
)



# Method 4: With timing
start_time=$(date +%s)

for script in "${SCRIPTS[@]}"; do
    script_start=$(date +%s)
    echo "[$(date '+%H:%M:%S')] Running $script..."
    
    python3 "$script"
    
    script_end=$(date +%s)
    echo "$script completed in $((script_end - script_start)) seconds"
done

end_time=$(date +%s)
echo "================================"
echo "Total time: $((end_time - start_time)) seconds"

