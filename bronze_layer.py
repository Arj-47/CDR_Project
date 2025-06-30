from shared_config import get_spark_session, pg_url, pg_properties

spark = get_spark_session("CDR Bronze Layer")

df_raw = spark.read.csv(
    r"C:\Users\arjun\Downloads\cdr_og1.csv",
    header=True,
    inferSchema=True
)

df_raw.write.jdbc(url=pg_url, table="bronze.raw_cdr", mode="overwrite", properties=pg_properties)

spark.stop()
