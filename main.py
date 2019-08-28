import os
import sys
import subprocess
import multiprocessing
from datetime import datetime

import numpy as np
import pandas as pd

storage_path = r"G:\dev\ActiveNetDataset"

def download_test(index, row):
    print("[{}] {} {} {} {} {}".format(
        datetime.now().strftime("%H:%M:%S"), index, 
        row["youtube_id"], row["time_start"], row["time_end"], row["split"]))
    tpath = os.path.join(storage_path, "temp", row["split"], "%s.%%(ext)s" % row["youtube_id"])
    fpath = os.path.join(storage_path, row["split"], "%s.%%(ext)s" % row["youtube_id"])
    
    if not os.path.exists(os.path.dirname(tpath)):
        os.makedirs(os.path.dirname(tpath))
    if not os.path.exists(os.path.dirname(fpath)):
        os.makedirs(os.path.dirname(fpath))
        
    tpath_real = tpath % {"ext": "mp4"}
    fpath_real = fpath % {"ext": "mp4"}
    if os.path.exists(fpath_real):
        return
        
    command1 = [
        "youtube-dl",
        "-icw",
        "--quiet", "--no-warnings",
        "-f", "mp4",
        "-o", "\"%s\"" % tpath,
        "\"https://youtu.be/%s\"" % row["youtube_id"]
    ]
    command1 = " ".join(command1)
    try:
        subprocess.check_output(command1, shell=False)
    except Exception as err:
        print("[{}] {}".format(datetime.now().strftime("%H:%M:%S"), err))
    
    if os.path.exists(fpath_real):
        return
    command2 = [
        "ffmpeg",
        "-i", "\"%s\"" % tpath % {"ext": "mp4"},
        "-ss", str(row["time_start"]),
        "-t", str(row["time_end"] - row["time_start"]),
        "-c:v", "libx264", "-c:a", "copy",
        "-threads", "1",
        "-loglevel", "panic",
        "\"%s\"" % fpath_real
    ]
    command2 = " ".join(command2)
    try:
        subprocess.check_output(command2, shell=False)
    except Exception as err:
        print("[{}] {}".format(datetime.now().strftime("%H:%M:%S"), err))

def download_train_or_val(index, row):
    print("[{}] {} {} {} {} {} {}".format(
        datetime.now().strftime("%H:%M:%S"), index, 
        row["youtube_id"], row["time_start"], row["time_end"], row["split"], row["label"]))
    tpath = os.path.join(storage_path, "temp", row["split"], row["label"], "%s.%%(ext)s" % row["youtube_id"])
    fpath = os.path.join(storage_path, row["split"], row["label"], "%s.%%(ext)s" % row["youtube_id"])
    
    if not os.path.exists(os.path.dirname(tpath)):
        os.makedirs(os.path.dirname(tpath))
    if not os.path.exists(os.path.dirname(fpath)):
        os.makedirs(os.path.dirname(fpath))
    
    tpath_real = tpath % {"ext": "mp4"}
    fpath_real = fpath % {"ext": "mp4"}
    if os.path.exists(fpath_real):
        return
    
    command1 = [
        "youtube-dl",
        "--quiet", "--no-warnings",
        "-f", "mp4",
        "-o", "\"%s\"" % tpath,
        "\"https://youtu.be/%s\"" % row["youtube_id"]
    ]
    command1 = " ".join(command1)
    try:
        subprocess.check_output(command1, shell=False)
    except Exception as err:
        print(err)
    
    if os.path.exists(fpath_real):
        return
    command2 = [
        "ffmpeg",
        "-i", "\"%s\"" % tpath % {"ext": "mp4"},
        "-ss", str(row["time_start"]),
        "-t", str(row["time_end"] - row["time_start"]),
        "-c:v", "libx264", "-c:a", "copy",
        "-threads", "1",
        "-loglevel", "panic",
        "\"%s\"" % fpath_real
    ]
    command2 = " ".join(command2)
    try:
        subprocess.check_output(command2, shell=False)
    except Exception as err:
        print(err)

# ====================== Run on thread pool ========================

def download_test_(x):
    download_test(x[0], x[1])

def download_train_or_val_(x):
    download_train_or_val(x[0], x[1])

if __name__ == "__main__":
    with multiprocessing.Pool(processes=12) as pool:
        df_test = pd.read_csv("./kinetics_700_test.csv")
        tasks_test = df_test.iterrows()
        pool.map(download_test_, tasks_test)
        
        df_train = pd.read_csv("./kinetics_700_train.csv")
        tasks_train = df_train.iterrows()
        pool.map(download_train_or_val_, tasks_train)
        
        #df_val = pd.read_csv("./kinetics_700_val.csv")
        #tasks_val = df_val.iterrows()
        #pool.map(download_train_or_val_, tasks_val)
        
        
        
