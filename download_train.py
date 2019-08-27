import os
import sys
import subprocess

import numpy as np
import pandas as pd

storage_path = r"G:\dev\ActiveNetDataset"


# download val
df_val = pd.read_csv("./kinetics_700_val.csv")
for index, row in df_val.iterrows():
    print(index, row["youtube_id"], row["time_start"], row["time_end"], row["split"], row["label"])
    tpath = os.path.join(storage_path, "temp", row["split"], row["label"], "%s.%%(ext)s" % row["youtube_id"])
    fpath = os.path.join(storage_path, row["split"], row["label"], "%s.%%(ext)s" % row["youtube_id"])
    tpath_real = tpath % {"ext": "mp4"}
    fpath_real = fpath % {"ext": "mp4"}
    if os.path.exists(tpath_real) and os.path.exists(fpath_real):
        continue
    command1 = [
        "youtube-dl",
        "--quiet", "--no-warnings",
        "-f", "mp4",
        "-o", "\"%s\"" % tpath,
        "\"https://youtu.be/%s\"" % row["youtube_id"]
    ]
    command1 = " ".join(command1)
    #print(command1)
    #os.system(command1)
    try:
        subprocess.check_output(command1, shell=False)
    except Exception as err:
        print(err)
    
    command2 = [
        "ffmpeg",
        "-i", "\"%s\"" % tpath % {"ext": "mp4"},
        "-ss", str(row["time_start"]),
        "-t", str(row["time_end"] - row["time_start"]),
        "-c:v", "libx264", "-c:a", "copy",
        #"-threads", "1",
        "-loglevel", "panic",
        "\"%s\"" % fpath_real
    ]
    command2 = " ".join(command2)
    #print(command2)
    #os.system(command2)
    try:
        subprocess.check_output(command2, shell=False)
    except Exception as err:
        print(err)
