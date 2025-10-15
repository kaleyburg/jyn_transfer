#%% importing packages
import json
from datetime import datetime
import requests
#from bs4 import BeautifulSoup
import pandas as pd # data manipulation 
#import praw # python reddit API wrapper
#from praw import Reddit
import pprint
import pandas as pd
import numpy as np
import functools
print = functools.partial(print, flush=True)
import re
import os
import praw
from collections import defaultdict
import subprocess
import csv
import zstandard as zstd
import matplotlib.pyplot as plt
import subprocess
import shutil
from typing import List
from glob import glob
import time
import psutil
import datetime
#import openpyxl


#%% get_file_indices

def get_file_indices(csv_file: str, base_name: str, ends_with: str = None) -> list[int]:
    """
    Finds all file indices in a torrent CSV where the filename starts with a given base_name
    (before the first underscore) and optionally ends with a specific suffix.

    :param csv_file: Path to CSV with columns: index, path
    :param base_name: The base name to match before the first underscore
    :param ends_with: Optional suffix to match
    :return: List of 1-based indices of matching files
    """
    indices = []
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            index = int(row['index'])
            filename = row['path'].split('/')[-1] 
            if not filename.endswith(ends_with):
                continue
            # remove the suffix and compare
            name_part = filename[:-len(ends_with)]
            if name_part != base_name:
                continue
            
            indices.append(index)
    
    if not indices:
        raise ValueError(f"No matching files found for base name '{base_name}'")
    
    return indices

#get_file_indices(csv_file='torr_file_names.csv', base_name='Civcraft_Orion', ends_with='_submissions.zst')

#%% get_file_names (from indices in csv)
def get_file_names(csv_file: str, indices: List[int]) -> List[str]:
    """
    Returns the filenames corresponding to the given indices in the CSV.

    :param csv_file: Path to CSV with columns: index, path
    :param indices: List of 1-based indices to look up
    :return: List of filenames (just the last part, without directories)
    """
    indices_set = set(indices)  # for faster lookup
    file_names = []

    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            index = int(row['index'])
            if index in indices_set:
                filename = row['path'].split('/')[-1]  # just the filename
                file_names.append(filename)

    if not file_names:
        raise ValueError(f"No filenames found for indices: {indices}")
    
    return file_names



#%% download_torrent_file
def download_torrent_file(torrent_file: str, file_index: list[int], output_folder: str):
    try:
        cmd = [
            "aria2c",
            "-s16", "-x16",
            f"--select-file={file_index}",
            "--seed-time=0",
            f"--dir={output_folder}",
            torrent_file
        ]
        subprocess.run(cmd, check=True)
        print(f"File with index {file_index} downloaded successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error downloading file index {file_index}: {e}")
        
        
#test comments
#download_torrent_file(torrent_file='dataset.torrent', file_index=66069)       

#test subs
#download_torrent_file(torrent_file='dataset.torrent', file_index=66070)      


#%% parse_base_name - get subreddit names

def parse_base_name(path: str) -> str:
    """Extracts the base name (e.g. 'mousehunt' from 'reddit/subreddits/mousehunt_comments.zst').
    Requires the path name that contains the file"""
    name = os.path.basename(path)
    name = name.replace("_comments.zst", "").replace("_submissions.zst", "")
    return name

#%% delete_folder

def delete_folder(folder_path: str):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        print(f"Deleted folder: {folder_path}")
    else:
        print(f"Folder not found: {folder_path}")
           
#delete_folder('reddit')


#%% stream_zst_lines

def stream_zst_lines(file_path):
    """gets JSON lines from zst files without fully decompressing them"""
    with open(file_path, 'rb') as fh:
        #change the window size maybe later? idk
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        with dctx.stream_reader(fh) as reader:
            buffer = ''
            while True:
                chunk = reader.read(2**27)
                if not chunk:
                    break
                buffer += chunk.decode('utf-8', errors='ignore')
                lines = buffer.split('\n')
                for line in lines[:-1]:
                    yield line
                buffer = lines[-1]
            if buffer:
                yield buffer



#%% process_comments

def process_comments(file_path: str, usernames_csv: str, comments_full: list):
    error_msg = None
    try:
        with open(usernames_csv, newline='', encoding='utf-8') as f_csv:
            reader = csv.DictReader(f_csv)
            usernames = {row['author'] for row in reader}
            
                    
        total_lines = 0
        total_user_matches = 0
        total_top_level = 0
        total_kept = 0
        
        for line in stream_zst_lines(file_path):
            total_lines += 1
            try:
                obj = json.loads(line)
                author = obj.get("author")
                if author in usernames:
                    total_user_matches += 1
                    #makes sure it only collects usernames that we also have data on
                    parent_id = obj.get("parent_id")
                    link_id = obj.get("link_id")
                    subreddit = obj.get("subreddit")
                    parentid_clean = parent_id.split("_")[-1]
                    body = obj.get("body")
                    post_info = {
                        "author": author,
                        "subreddit": subreddit,
                        "body": body,
                        "parent_id": obj.get("parent_id"),
                        "link_id": obj.get("link_id"),
                        "parentid_clean": parentid_clean
                    }
                    
                                    # Ensure only top-level comments are kept
                    if not parent_id.startswith("t3_"):
                       # print(f"Skipping non-top-level comment: parent_id={parent_id}")
                        continue
                    total_top_level += 1
                    if not all([author, parent_id, link_id, subreddit, body]):
                        continue
                
                
                    if parent_id == link_id:
                      #ensures that this is only appended if the comment replies to the submission post
                        comments_full.append(post_info)
                        total_kept += 1
                     #   print('replying to another comment')

                  #  matched_subreddits.append(filename)
                    # Stop scanning this file — we only need one
                    
            except json.JSONDecodeError:
                continue
            
   #     print(f"Processed file: {file_path}")
   #     print(f"  Total lines read: {total_lines}")
   #     print(f"  Comments matching usernames: {total_user_matches}")
   #     print(f"  Top-level comments (t3_): {total_top_level}")
   #     print(f"  Comments appended (kept): {total_kept}")
   #     print(f"  Percentage kept after filtering: {total_kept / total_user_matches * 100:.2f}%\n" if total_user_matches else "")  
         
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        error_msg = str(e)
        
    # always return stats, even if exception occurred
    return {
        "total_lines": total_lines,
        "total_user_matches": total_user_matches,
        "total_top_level": total_top_level,
        "total_kept": total_kept,
        "error": error_msg
    }
#test section 
#all_comments = [] 
#process_comments(file_path='reddit/subreddits24/mousehunt_comments.zst', usernames_csv='users.csv', comments_full=all_comments)


    
    
#%% process_submissions

def process_submissions(file_path: str, comments_full: list, submissions_full: list):
    parentids_clean = {c['parentid_clean'] for c in comments_full if 'parentid_clean' in c}

    total_matches = 0
    filtered_out_link_posts = 0
    filtered_out_empty_text = 0
    filtered_out_deleted = 0
    kept = 0
    error_msg = None
    
    try:
        for line in stream_zst_lines(file_path):
            try:
                obj = json.loads(line)
                parentid = obj.get("id")
                selftext = obj.get('selftext')
                subreddit = obj.get("subreddit")
                is_self = obj.get("is_self", False)
                title = obj.get('title')
                
                if parentid in parentids_clean:
                    total_matches += 1
                    
                    if not is_self:
                        filtered_out_link_posts += 1
                        continue
                    
                    if not selftext:
                        filtered_out_empty_text += 1
                        continue
                        
                    if selftext in ["[deleted]", "[removed]"]:
                        filtered_out_deleted += 1
                        continue
                    
                    post_info = {
                        "author": obj.get('author'),
                        "subreddit": subreddit,
                        "selftext": selftext,
                        "id": obj.get("id"),
                        'title': title
                    }
                    submissions_full.append(post_info)
                    kept += 1

            except json.JSONDecodeError:
                print('boop')
                continue
                
   #     print(f"\nSubmission filtering stats for {file_path}:")
    #    print(f"  Total submissions matching comment parent IDs: {total_matches}")
     #   print(f"  Filtered out - link posts (is_self=False): {filtered_out_link_posts}")
      #  print(f"  Filtered out - empty text: {filtered_out_empty_text}")
      #  print(f"  Filtered out - deleted/removed: {filtered_out_deleted}")
      #  print(f"  Kept: {kept}")
      #  if total_matches > 0:
      #      print(f"  Percentage kept: {kept / total_matches * 100:.2f}%\n")
        
    
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        error_msg = str(e)
        
        # Return stats directly
    return {
        "total_matches": total_matches,
        "filtered_out_link_posts": filtered_out_link_posts,
        "filtered_out_empty_text": filtered_out_empty_text,
        "filtered_out_deleted": filtered_out_deleted,
        "kept": kept,
        "error": error_msg
    }
    
#test section
   
#all_submissions = []   
#process_submissions(file_path='reddit/subreddits24/mousehunt_submissions.zst', usernames_csv='users.csv', comments_full=all_comments, submissions_full=all_submissions)


#%% merge_coms_subs

def merge_coms_subs(comments_full: list, submissions_full: list, merged: list):
    # Create a dictionary for quick lookup of submissions by their ID
    submissions_dict = {j.get("id"): j for j in submissions_full}
    print(f"Total submissions available for merging: {len(submissions_dict)}")
    
    unmatched_comments = []
    
      
    for i in comments_full:
        iid = i.get("parentid_clean")
        if iid in submissions_dict:
            # Get the matching submission
            j = submissions_dict[iid]
            merged_entry = i.copy()
            merged_entry.update({
                "submission_author": j.get("author"),
                "submission_subreddit": j.get("subreddit"),
                "submission_selftext": j.get("selftext"),
                "submission_title": j.get("title"),
                "submission_id": j.get('id')
            })
            merged.append(merged_entry)

            
#test section
            
#subs_and_coms = []
#merge_coms_subs(comments_full=all_comments, submissions_full=all_submissions, merged=subs_and_coms)


#%% create_subreddit_index_csv 


def create_subreddit_index_csv(torr_csv: str, out_csv: str):
    
    grouped = {}

    # Group all files by base_name
    with open(torr_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            idx = int(row["index"])
            path = row["path"]
            base_name = parse_base_name(path)
            grouped.setdefault(base_name, []).append((path, idx))

    # Filter & format
    cleaned = []
    skipped = 0

    for base_name, entries in grouped.items():
        if len(entries) != 2:
            skipped += 1
            continue  # must have exactly two files

        # Sort to ensure _comments first, _submissions second
        entries_sorted = sorted(
            entries,
            key=lambda x: (not x[0].endswith("_comments.zst"), x[0])
        )

        comments, submissions = entries_sorted
        if not comments[0].endswith("_comments.zst") or not submissions[0].endswith("_submissions.zst"):
            skipped += 1
            continue  # wrong pairing, skip

        indices = [comments[1], submissions[1]]
        cleaned.append((base_name, ",".join(map(str, indices))))

    # Write the output
    with open(out_csv, 'w', newline='', encoding='utf-8') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(["base_name", "indices"])
        writer.writerows(cleaned)

    print(f"✅ Created {out_csv}")
    print(f"   Total valid subreddits: {len(cleaned)}")
    print(f"   Skipped invalid/missing pairs: {skipped}")



# creation of the file
#create_subreddit_index_csv('torr_file_names.csv', 'pair_indices.csv')
#pairs = pd.read_csv('pair_indices.csv')

#%% get file size

def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size

#%% process_torrents

def process_torrents(start_i: int, end_i: int, pair_csv: str, reddit_folder: str):
    
    processed_basenames = set()
    merged_dataset = []
    log_records = []
    

    # main loop
    with open(pair_csv, newline='', encoding='utf-8') as f_pairs:
        reader = list(csv.DictReader(f_pairs))
        
        if start_i is not None and end_i is not None:
            reader = reader[start_i:end_i]

        for row in reader:
            
            base_name = row["base_name"]
            if base_name in processed_basenames:
                continue  # skip duplicates  

            indices = [int(i) for i in row["indices"].split(",")]
            file_index_str = ",".join(map(str, indices))
            
            start_time = time.time()
            
            comment_stats = {
                "total_kept": 0,
                "total_user_matches": 0,
                "total_top_level": 0,
                "error": None
            }
            submission_stats = {
                "kept": 0,
                "total_matches": 0,
                "filtered_out_link_posts": 0,
                "filtered_out_empty_text": 0,
                "filtered_out_deleted": 0,
                "error": None
            }
                
            #download files
            download_torrent_file(torrent_file='dataset.torrent', file_index=file_index_str, output_folder= reddit_folder)
            
            #reddit folder should be like: "C:\Users\Kaley\Documents\job1" 
            
            com_path = f"{reddit_folder}/reddit/subreddits24/{base_name}_comments.zst"
            sub_path = f"{reddit_folder}/reddit/subreddits24/{base_name}_submissions.zst"
            
            all_submissions = [] 
            all_comments = []
            
            comment_stats = process_comments(file_path=com_path, usernames_csv='users.csv', comments_full=all_comments)
        
            print(f"Processed comments for {base_name}, total comments so far: {len(all_comments)}")
            
            if len(all_comments) == 0:
                print(f"⚠️ No comments kept for {base_name}. Skipping submissions and merging.\n")
                
                reddit_subfolder = os.path.join(reddit_folder, "reddit")
                folder_size = round(get_size(reddit_subfolder))
                
                existing_files = []
                for root, dirs, files in os.walk(reddit_subfolder):
                    for file in files:
                        existing_files.append(os.path.relpath(os.path.join(root, file), reddit_subfolder))

                delete_folder(folder_path=os.path.join(reddit_folder, "reddit"))
                
                log_records.append({
                    "base_name": base_name,
                    "indices": indices,
                    "lines_in_csv": 0,
                    "comments_kept": comment_stats["total_kept"],
                    "com_total_user_matches": comment_stats["total_user_matches"],
                    "com_total_top_level": comment_stats["total_top_level"],
                    "submissions_kept": 0,
                    "sub_total_matches": 0,
                    "sub_filtered_out_link_posts": 0,
                    "sub_filtered_out_empty_text": 0,
                    "sub_filtered_out_deleted": 0,
                    "error_comments": comment_stats.get("error"),
                    "error_submissions": None,
                    "processing_time": round(time.time() - start_time, 2),
                    "cpu_percent": psutil.cpu_percent(interval=None),
                    "storage_used": round(psutil.disk_usage(os.getcwd()).used / (1024*1024), 2),
                    "folder_size": folder_size,
                    "files_in_folder": existing_files
                })
                continue  # jump to the next base_name
                    
            submission_stats = process_submissions(file_path:=sub_path, comments_full=all_comments, submissions_full=all_submissions)
            print(f"Processed submissions for {base_name}, total submissions so far: {len(all_submissions)}")
            
            merged_before = len(merged_dataset)
            merge_coms_subs(comments_full=all_comments, submissions_full=all_submissions, merged=merged_dataset)
            merged_after = len(merged_dataset)
            lines_contributed = merged_after - merged_before
            print(f"Merged dataset for {base_name}, added {lines_contributed} lines, total merged items: {merged_after}")
            
            # mark this base_name as processed
            processed_basenames.add(base_name)
            
            
            reddit_subfolder = os.path.join(reddit_folder, "reddit")
            folder_size = round(get_size(reddit_subfolder))
                
            existing_files = []
            for root, dirs, files in os.walk(reddit_subfolder):
                for file in files:
                    existing_files.append(os.path.relpath(os.path.join(root, file), reddit_subfolder))

            delete_folder(folder_path=os.path.join(reddit_folder, "reddit"))
                
                
            #logging info after successful iteration 
            end_time = time.time()
            log_records.append({
                    "base_name": base_name,
                    "indices": indices,
                    "lines_in_csv": lines_contributed,
                    "comments_kept": comment_stats["total_kept"],
                    "com_total_user_matches": comment_stats["total_user_matches"],
                    "com_total_top_level": comment_stats["total_top_level"],
                    "submissions_kept": submission_stats["kept"],
                    "sub_total_matches": submission_stats["total_matches"],
                    "sub_filtered_out_link_posts": submission_stats["filtered_out_link_posts"],
                    "sub_filtered_out_empty_text": submission_stats["filtered_out_empty_text"],
                    "sub_filtered_out_deleted": submission_stats["filtered_out_deleted"],
                    "error_comments": comment_stats.get("error"),
                    "error_submissions": submission_stats.get("error"),
                    "processing_time": round(end_time - start_time, 2),
                    "cpu_percent": psutil.cpu_percent(interval=0.1),
                    "storage_used": round(psutil.disk_usage(os.getcwd()).used / (1024*1024), 2),
                    "folder_size": folder_size,
                    "files_in_folder": existing_files
        })
      
    #delete batch folder
    delete_folder(folder_path=reddit_folder)

    
    #save everything          
    
    # Ensure the CSV output folder exists or make it
    csv_dir = os.path.join(os.getcwd(), "csvs")
    os.makedirs(csv_dir, exist_ok=True)
    
    out_csv = os.path.join(csv_dir, f"merged_{start_i or 0}-{end_i or 'end'}.csv")
    
    if merged_dataset:  # only save if there's data
        df = pd.DataFrame(merged_dataset)
        df.to_csv(out_csv, index=False, encoding='utf-8')
        
        # Update all log records to reflect CSV was saved
        for record in log_records:
            if record["lines_in_csv"] > 0:  # Check each record's own contribution
                record["csv_saved"] = True
    
    if log_records:     
        df_logs = pd.DataFrame(log_records)

        log_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        # Save to Excel
        excel_path = os.path.join(log_dir, f"processing_log_{start_i}-{end_i}.xlsx")
        df_logs.to_excel(excel_path, index=False)
        print(f"✅ Processing log saved to {excel_path}")
        
    


#fix where the log_records thing gets saved
# %% merged_csvs

def merge_csvs_to_final(folder_path: str, final_csv_name: str) -> pd.DataFrame:
    """
    Merge all CSV files in a folder and save the merged result as final_csv_name.
    
    Parameters:
    - folder_path: path to the folder containing CSV files
    - final_csv_name: name of the output merged CSV (default: 'final_csv.csv')
    
    Returns:
    - merged_df: pandas DataFrame containing all merged CSVs
    """
    folder_path = os.path.abspath(folder_path)
    
    # Find all CSV files in the folder
    csv_files = glob(os.path.join(folder_path, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {folder_path}")
        return pd.DataFrame()  # empty DataFrame
    
    # Read and concatenate all CSVs
    df_list = [pd.read_csv(f) for f in csv_files]
    merged_df = pd.concat(df_list, ignore_index=True)
    
    # Ensure folder exists for saving
    output_file = os.path.join(folder_path, final_csv_name)
    
    merged_df.to_csv(output_file, index=False, encoding='utf-8')
    
    print(f"✅ Merged {len(csv_files)} CSV files into {output_file} with total {len(merged_df)} rows.")
    
    return merged_df
