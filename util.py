
import pandas as pd
import os
import time

import pdb


def print_process(size: int, i: int, start: float):
    percentage = lambda i: (1/size*i)
    prcent = percentage(i)
    print(
        f"Batch #{i} -",
        "{:.2f}%".format(prcent*100), 
        "{:.2f}sec".format(time.time()-start), 
        "#"*int(50*prcent), end="\r"
    )


def get_parquet_df(name: str):
    data = pd.read_parquet(name, engine="pyarrow")
    return data
    # return data.to_dict(orient='records')


def get_parquet_names(folder):
    result = [[]]
    path = f"{os.path.abspath('.')}/data/{folder}"
    for dir in os.listdir(path):
        dir_path = f"{path}/{dir}"
        if os.path.isdir(dir_path):
            for parquet in os.listdir(dir_path):
                parquet_path = f"{dir_path}/{parquet}"
                if os.path.isfile(parquet_path):
                    result.append(parquet_path)
    
    return result


def get_df_from_parquet_batch(size: int = None, offset: int = 0, folder: str = "open-save"):
    dfs = []
    paths = get_parquet_names(folder)

    start = time.time()
    count = 0

    size = len(paths) if size is None else size

    for i in range(offset, offset+size):
        print_process(size, count, start)
        count += 1

        df = get_parquet_df(paths[i])
        dfs.append(df)
    
    print(" "*100, end="\r")
    print("Merging data...")
    return pd.concat(dfs)


def write_run(data):
    print("Writing db display to file...", end="\r")
    file = open("run.txt", "w")
    file.write(data)
    file.close()
    print("Done!")
