#%% batch job running

from concurrent.futures import ThreadPoolExecutor, as_completed
from functions import process_torrents  # import your function
import os
import functions 
from functions import *
import pandas as pd
import psutil
import time

def run_batches(pair_csv: str, total_pairs: int, batch_size: int, max_workers: int):
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
            end_i=end_i
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
    # Adjust these values as needed
    run_batches(
        pair_csv="pair_indices.csv",
        total_pairs=22,   # total subreddit pairs
        batch_size=4,      # how many per batch
        max_workers=4        # number of parallel threads (6 is safe for I/O)
    )




#get total length: 39880

#df = pd.read_csv('pair_indices.csv')
#len(df)