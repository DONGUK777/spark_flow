from airflow_spark.re import re_partition
import pandas as pd

def test_re_partition():
    df_row_cnt, read_path, write_path= re_partition("20150102")
    print(df_row_cnt)
    print(read_path)
    print(write_path)
    assert df_row_cnt == 40
