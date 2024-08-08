import pandas as pd
import os

def repartiton(load_dt, from_path='/data/moviedata/data/extract'):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/{from_path}/load_dt={load_dt}'
    write_path = f'{home_dir}/repartition'
    df = pd.read_parquet(read_path)
    df['load_dt'] = load_dt
    df.to_parquet(
            write_path,
            partition_cols=['load_dt', 'multiMovieYn', 'repNationCd']
            )
    return df.size, read_path, f'{write_path}/load_dt={load_dt}'

