import functions 
from functions import *


if __name__ == "__main__":
    print("Merging all batch CSVs into final...")
    merge_csvs_to_final(folder_path="csvs", final_csv_name="final_csv.csv")
    print("✅ Final merged CSV saved to csvs/final_csv.csv")