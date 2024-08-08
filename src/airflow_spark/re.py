import pandas as pd
import os
import shutil

def re_partition(load_dt, from_path='/data/movie_data/data/extract'):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/{from_path}/load_dt={load_dt}'
    write_base = f'{home_dir}/data/movie_data/repartition'
    write_path = f'{write_base}/load_dt={load_dt}'
    
    df = pd.read_parquet(read_path)
    df['load_dt'] = load_dt
    rm_dir(write_path)

    df.to_parquet(
            write_base,
            partition_cols=['load_dt', 'multiMovieYn', 'repNationCd']
            )

    return len(df), read_path, write_path

def rm_dir(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
