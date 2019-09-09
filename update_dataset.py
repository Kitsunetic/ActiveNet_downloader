import argparse
import typing
import os
import sys

import numpy as np
import pandas as pd


def check_downloaded(dpath: str, item: pd.core.series.Series) -> bool:
    if 'label' in item and item['label']:
        fpath = os.path.join(dpath, item['split'], item['label'], item['youtube_id'] + '.mp4')
    else:
        fpath = os.path.join(dpath, item['split'], item['youtube_id'] + '.mp4')
    # File exsistence condition:
    #     1. file exists
    #     2. file size is larger than 100KB
    return os.path.exists(fpath) and os.path.getsize(fpath) > 102400
    
def reduce_dataset(dpath: str, dataset_file: str) -> pd.core.frame.DataFrame:
    df = pd.read_csv(dataset_file)
    if 'label' not in df:
        df['label'] = pd.Series(np.array([None]*len(df['youtube_id'])), index=df.index)
    
    dropping_indexies = []
    for i, row in df.iterrows():
        if check_downloaded(dpath, row):
            dropping_indexies.append(i)
    return df.drop(df.index[dropping_indexies])

def update(dpath: str, dataset_files: typing.List[str], N: int):
    # Create concatenated dataframe
    df_list = map(lambda dataset_file: reduce_dataset(dpath, dataset_file), dataset_files)
    df = pd.concat(list(df_list), ignore_index=True, sort=False)
    
    # Split dataframe
    L = len(df['youtube_id'])
    mask_base = np.random.rand(L)
    for i in range(1, N+1):
        fname = './kinetics_700_%d.csv'%i
        print('Create concatenated dataset', fname)
        df_split = df[((i-1)/N < mask_base) & (mask_base < i/N)]
        df_split.to_csv(fname)
    
    del df
    del df_list

def main():
    parser = argparse.ArgumentParser(description=
                'Drop already downloaded items and concat datasets to randomly sorted N\'th csv files')
    parser.add_argument('-n', '--num_split', type=int, default=1, help='number of splitted output csv files[1]')
    parser.add_argument('--storage_path', type=str, default='./', help='path where downloaded datasets are stored[./]')
    parser.add_argument('dataset_file', nargs='*', type=str, help='dataset file names to cotcatenate')
    p = parser.parse_args(sys.argv[1:])
    if not p.dataset_file:
        parser.print_help()
        return
    
    update(p.storage_path, p.dataset_file, p.n)

if __name__ == "__main__":
    main()
    
    # 오래된 csv파일들은 ./original/old_dataset/ymdhms 폴더에 옮기기
    