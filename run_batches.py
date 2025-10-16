#%% batch job running

from concurrent.futures import ThreadPoolExecutor, as_completed
from functions import process_torrents  # import your function
import os
import functions 
from functions import *
import pandas as pd
import psutil
import time



def run_batches(pair_csv: str, total_pairs: int, batch_size: int, max_workers: int, skip_indices: list = None):
    """
    Run process_torrents() in parallel batches using threads.
    """
    def run_batch(start_i, end_i):
        reddit_folder = f"batch_{start_i}-{end_i}"
        print(f"Starting batch {reddit_folder}...")

        process_torrents(
            pair_csv=pair_csv,
            reddit_folder=reddit_folder,
            start_i=start_i,
            end_i=end_i,
            skip_indices=skip_indices
        )

        print(f"✅ Finished batch {reddit_folder}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for start_i in range(0, total_pairs, batch_size):
            end_i = min(start_i + batch_size, total_pairs - 1)
            futures.append(executor.submit(run_batch, start_i, end_i))

        # wait for all to complete
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print(f"❌ Error in batch: {e}")


    print("✅ All batches completed successfully.")




if __name__ == "__main__":
    # Load biggest_20 indices from CSV
    biggest_20_list = []
    try:
        with open('biggest_20.csv', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'index' in row:
                    biggest_20_list.append(int(row['index']))
        print(f"Loaded {len(biggest_20_list)} indices from biggest_20.csv: {biggest_20_list}")
    except FileNotFoundError:
        print("⚠️ biggest_20.csv not found. Proceeding without skip indices.")
    except Exception as e:
        print(f"⚠️ Error reading biggest_20.csv: {e}")
        
    # Adjust these values as needed
    run_batches(
        pair_csv="pair_indices.csv",
        total_pairs=39881,   # total subreddit pairs
        batch_size=10,      # how many per batch
        max_workers=100,      # number of concurrent threads
        skip_indices=biggest_20_list
    )




#get total length: 39880

#df = pd.read_csv('pair_indices.csv')
#len(df)

#pretty sure it is i/o bound so thread is the right choice
#but now not sure about RAM usage problems
#since cpu is so low do i want to divide it into smaller batches and run more workers at one time?