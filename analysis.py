import os 
import re
import pandas as pd
import ollama
from multiprocessing import Pool, cpu_count, Manager
import time
from tqdm import tqdm
import numpy as np
from functools import partial
import warnings
warnings.filterwarnings('ignore')

# IMPORTANT: Ollama client must be created in each process
def init_worker():
    """Initialize Ollama client for each worker process"""
    global client, model
    client = ollama.Client()
    model = "qwen3:8b"

def categorize_job(title):
    """
    Function to categorize a single job title.
    Client is created in each process via init_worker
    """
    prompt = f'''Which Job category does the {title} title belong to. Categories are Health/Medical Services, Software Development/Engineering, IT Infrastructure & Support, Business Operations & Analysis, Manufacturing & Technical Operations, Administrative & Support Services. Only return one Job Category and nothing else /no_think'''
    
    try:
        response = client.generate(model=model, prompt=prompt)
        category = response.response.replace("<think>", "").replace("</think>", "").strip()
        return category
    except Exception as e:
        print(f"Error processing title '{title}': {e}")
        return "Error"

def categorize_job_with_index(args):
    """
    Wrapper function that handles index and title
    """
    idx, title = args
    category = categorize_job(title)
    return idx, category

# Method 1: Simple Multiprocessing with Pool
def categorize_multiprocess_simple(df, n_processes=None):
    """
    Use multiprocessing Pool to process job titles across multiple cores
    """
    if n_processes is None:
        n_processes = min(cpu_count(), 8)  # Cap at 8 processes
    
    print(f"Using {n_processes} processes on {cpu_count()} available cores")
    
    titles = df['title'].tolist()
    
    with Pool(processes=n_processes, initializer=init_worker) as pool:
        categories = pool.map(categorize_job, titles)
    
    return categories

# Method 2: Multiprocessing with Progress Bar
def categorize_multiprocess_progress(df, n_processes=None):
    """
    Multiprocessing with progress tracking
    """
    if n_processes is None:
        n_processes = min(cpu_count(), 8)
    
    print(f"Using {n_processes} processes on {cpu_count()} available cores")
    
    # Prepare data with indices
    job_data = [(idx, row['title']) for idx, row in df.iterrows()]
    
    categories = [None] * len(df)
    
    with Pool(processes=n_processes, initializer=init_worker) as pool:
        # Use imap for progress tracking
        with tqdm(total=len(job_data)) as pbar:
            for idx, category in pool.imap_unordered(categorize_job_with_index, job_data):
                categories[idx] = category
                pbar.update()
    
    return categories

# Method 3: Chunk-based Multiprocessing for Better Load Balancing
def process_chunk(chunk_data):
    """
    Process a chunk of job titles
    """
    # Initialize client for this process if not already done
    if 'client' not in globals():
        init_worker()
    
    results = []
    for idx, title in chunk_data:
        category = categorize_job(title)
        results.append((idx, category))
    return results

def categorize_multiprocess_chunks(df, n_processes=None, chunk_size=None):
    """
    Process data in chunks for better load balancing
    """
    if n_processes is None:
        n_processes = min(cpu_count(), 8)
    
    if chunk_size is None:
        chunk_size = max(1, len(df) // (n_processes * 4))
    
    print(f"Using {n_processes} processes with chunk size {chunk_size}")
    
    # Prepare data
    job_data = [(idx, row['title']) for idx, row in df.iterrows()]
    
    # Create chunks
    chunks = [job_data[i:i+chunk_size] for i in range(0, len(job_data), chunk_size)]
    
    # Process chunks in parallel
    with Pool(processes=n_processes, initializer=init_worker) as pool:
        chunk_results = list(tqdm(
            pool.imap(process_chunk, chunks),
            total=len(chunks),
            desc="Processing chunks"
        ))
    
    # Flatten and sort results
    all_results = []
    for chunk_result in chunk_results:
        all_results.extend(chunk_result)
    
    all_results.sort(key=lambda x: x[0])
    categories = [cat for _, cat in all_results]
    
    return categories

# Method 4: Hybrid Approach - Multiprocessing + Threading
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process, Queue

def worker_process(process_id, job_queue, result_queue, n_threads=2):
    """
    Worker process that uses threads internally
    """
    # Initialize Ollama client for this process
    client = ollama.Client()
    model = "qwen3:8b"
    
    def thread_categorize(title):
        prompt = f'''Which Job category does the {title} title belong to. Categories are Health/Medical Services, Software Development/Engineering, IT Infrastructure & Support, Business Operations & Analysis, Manufacturing & Technical Operations, Administrative & Support Services. Only return one Job Category and nothing else /no_think'''
        
        try:
            response = client.generate(model=model, prompt=prompt)
            category = response.response.replace("<think>", "").replace("</think>", "").strip()
            return category
        except Exception as e:
            return "Error"
    
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        while True:
            job = job_queue.get()
            if job is None:  # Poison pill
                break
            
            idx, title = job
            category = thread_categorize(title)
            result_queue.put((idx, category))

def categorize_hybrid(df, n_processes=4, threads_per_process=2):
    """
    Hybrid approach using both multiprocessing and threading
    """
    manager = Manager()
    job_queue = manager.Queue()
    result_queue = manager.Queue()
    
    # Fill job queue
    for idx, row in df.iterrows():
        job_queue.put((idx, row['title']))
    
    # Add poison pills
    for _ in range(n_processes):
        job_queue.put(None)
    
    # Start worker processes
    processes = []
    for i in range(n_processes):
        p = Process(target=worker_process, args=(i, job_queue, result_queue, threads_per_process))
        p.start()
        processes.append(p)
    
    # Collect results
    categories = [None] * len(df)
    for _ in tqdm(range(len(df)), desc="Processing"):
        idx, category = result_queue.get()
        categories[idx] = category
    
    # Wait for processes to finish
    for p in processes:
        p.join()
    
    return categories

# Method 5: Async + Multiprocessing (if Ollama supports async)
import asyncio
import aiohttp

async def async_categorize(session, title, model="qwen3:8b"):
    """
    Async version for Ollama API calls (if supported)
    """
    prompt = f'''Which Job category does the {title} title belong to. Categories are Health/Medical Services, Software Development/Engineering, IT Infrastructure & Support, Business Operations & Analysis, Manufacturing & Technical Operations, Administrative & Support Services. Only return one Job Category and nothing else /no_think'''
    
    # This assumes Ollama has an HTTP API endpoint
    url = "http://localhost:11434/api/generate"  # Adjust based on your Ollama setup
    
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False
    }
    
    try:
        async with session.post(url, json=payload) as response:
            result = await response.json()
            category = result.get('response', '').replace("<think>", "").replace("</think>", "").strip()
            return category
    except Exception as e:
        print(f"Error: {e}")
        return "Error"

async def process_batch_async(titles, model="qwen3:8b"):
    """
    Process a batch of titles asynchronously
    """
    async with aiohttp.ClientSession() as session:
        tasks = [async_categorize(session, title, model) for title in titles]
        return await asyncio.gather(*tasks)

def categorize_async_multiprocess(df, n_processes=4, batch_size=10):
    """
    Combine async I/O with multiprocessing
    """
    def process_async_batch(titles):
        return asyncio.run(process_batch_async(titles))
    
    # Split data into batches
    titles = df['title'].tolist()
    batches = [titles[i:i+batch_size] for i in range(0, len(titles), batch_size)]
    
    # Process batches in parallel processes
    with Pool(processes=n_processes) as pool:
        batch_results = list(tqdm(
            pool.imap(process_async_batch, batches),
            total=len(batches),
            desc="Processing batches"
        ))
    
    # Flatten results
    categories = []
    for batch_result in batch_results:
        categories.extend(batch_result)
    
    return categories

# Main execution
if __name__ == "__main__":
    # Read data
    df = pd.read_csv("all_companies_job_details_last_15_days_20250904_032220.csv", dtype=str)
    
    print(f"Total jobs to categorize: {len(df)}")
    print(f"Available CPU cores: {cpu_count()}")
    
    # Record start time
    start_time = time.time()
    
    # Choose your preferred method:
    
    # Option 1: Simple multiprocessing (recommended for most cases)
    #categories = categorize_multiprocess_simple(df, n_processes=6)
    
    # Option 2: With progress bar
    #categories = categorize_multiprocess_progress(df, n_processes=6)
    
    # Option 3: Chunk-based processing
    # categories = categorize_multiprocess_chunks(df, n_processes=6)
    
    # Option 4: Hybrid (multiprocessing + threading)
    categories = categorize_hybrid(df, n_processes=4, threads_per_process=2)
    
    # Option 5: Async + Multiprocessing (if Ollama supports HTTP API)
    #categories = categorize_async_multiprocess(df, n_processes=4, batch_size=10)
    
    # Add categories to dataframe
    df["Category"] = categories
    
    # Save results
    df.to_csv("categorized_jobs_multicore.csv", index=False)
    
    # Print statistics
    end_time = time.time()
    print(f"\nProcessing completed!")
    print(f"Total time: {end_time - start_time:.2f} seconds")
    print(f"Average time per job: {(end_time - start_time)/len(df):.3f} seconds")
    print(f"Total jobs processed: {len(df)}")
    
    # Show category distribution
    print("\nCategory Distribution:")
    print(df["Category"].value_counts())

"""
PERFORMANCE COMPARISON & RECOMMENDATIONS:

1. THREADING vs MULTIPROCESSING:
   - Threading: Better for I/O-bound tasks (API calls)
   - Multiprocessing: Better for CPU-bound tasks, but can also help with I/O

2. FOR OLLAMA API CALLS:
   - Threading is usually MORE efficient because:
     * Lower overhead (threads share memory)
     * API calls are I/O-bound, not CPU-bound
     * Ollama server might be the bottleneck, not your CPU
   
3. WHEN TO USE MULTIPROCESSING:
   - When you have CPU-intensive preprocessing
   - When threading hits GIL limitations
   - When you need true parallelism

4. OPTIMAL CONFIGURATION:
   - For Ollama: 10-20 threads usually better than 4-8 processes
   - Hybrid approach might work best (4 processes × 3 threads = 12 concurrent)

5. BOTTLENECK CONSIDERATIONS:
   - Ollama server capacity (can it handle 20 concurrent requests?)
   - Network latency (local vs remote Ollama)
   - Model loading time (each process loads model separately)

6. BENCHMARKING SUGGESTION:
   Test with small subset (100 items) to find optimal configuration:
   - 1 thread (baseline)
   - 10 threads
   - 4 processes
   - 4 processes × 2 threads (hybrid)
"""