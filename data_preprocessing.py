import pandas as pd
import os

files = [file for file in os.listdir("./raw_data") if ".csv" in file]
for file in files:
    data = pd.read_csv("./raw_data/"+file)
    output_file = "./cleaned_data/"+file.replace("olist_","").replace("_dataset","").replace(".csv",".json")
    data.to_json(output_file,orient="records")