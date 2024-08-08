import pandas as pd
import os
import shutil

def repartiton(load_dt, from_path='/data/movie_data/data/extract'):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/{from_path}/load_dt={load_dt}'
    write_base = f'{home_dir}/data/movie_datarepartition'
    write_path = f'{write_path}/load_dt={load_dt}'
    df = pd.read_parquet(read_path)
    df['load_dt'] = load_dt
    df.to_parquet(
            write_path,
            partition_cols=['load_dt', 'multiMovieYn', 'repNationCd']
            )
    return len()df, read_path, f'{write_path}/load_dt={load_dt}'

def rm_dir(dir_path):
    if os.path.exits(dir_path):
        shutil.rmtree(dir_path)
