from shared_config import get_spark_session, pg_url, pg_properties
from pyspark.sql.functions import col, when

spark = get_spark_session("CDR Silver Layer")

df_raw = spark.read.jdbc(url=pg_url, table="bronze.raw_cdr", properties=pg_properties)

df_clean = df_raw \
    .filter((col("A_Number").isNotNull()) & (col("B_Number").isNotNull()) & (col("Date").isNotNull())) \
    .withColumnRenamed("A_Number", "a_number") \
    .withColumnRenamed("B_Number", "b_number") \
    .withColumnRenamed("Originating_MDL_Location", "origin_country") \
    .withColumnRenamed("Originating_MDL_Dial_Code", "origin_code") \
    .withColumnRenamed("MDL_Destination", "destination_country") \
    .withColumnRenamed("MDL_Destination_Dial_Code", "destination_code") \
    .withColumnRenamed("Date", "call_date") \
    .withColumnRenamed("Time", "call_time") \
    .withColumnRenamed("Release_Cause", "release_cause") \
    .withColumn("call_status", when(col("Call_Status") == 'A', "Answered")
                                .when(col("Call_Status") == 'U', "Unanswered")
                                .otherwise("Other")) \
    .withColumn("call_duration_seconds", col("Unanswered_Time").cast("double")) \
    .select("a_number", "b_number", "origin_country", "origin_code", "destination_country",
            "destination_code", "call_date", "call_time", "call_status",
            "release_cause", "call_duration_seconds")

df_clean.write.jdbc(url=pg_url, table="silver.cleaned_cdr", mode="overwrite", properties=pg_properties)

spark.stop()
