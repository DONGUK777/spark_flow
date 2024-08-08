from pyspark.sql import SparkSession
import sys
LOAD_DT = sys.argv[1]

spark = SparkSession.builder.appName("aggDF").getOrCreate()

df1 = spark.read.parquet(f"/home/tommy/data/movie_data/hive/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("oneday_hive")

df_m = spark.sql(f"""
SELECT
    sum(salesAmt) as sum_salesAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    multiMovieYn,
    '{LOAD_DT}' AS load_dt
FROM oneday_hive
GROUP BY multiMovieYn
""")

df_m.write.mode('append').partitionBy("load_dt").parquet("/home/tommy/data/movie_data/sum-multi")

df_n = spark.sql(f"""
SELECT
    sum(salesAmt) as sum_salesAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    repNationCd,
    '{LOAD_DT}' AS load_dt
FROM oneday_hive
GROUP BY repNationCd
""")

df_n.write.mode('append').partitionBy("load_dt").parquet("/home/tommy/data/movie_data/sum-nation")

spark.stop()
