from shared_config import get_spark_session, pg_url, pg_properties
from pyspark.sql.functions import col, count, when, expr, avg, hour, substring

spark = get_spark_session("CDR Gold Layer")

df_clean = spark.read.jdbc(url=pg_url, table="silver.cleaned_cdr", properties=pg_properties)

# KPI 1
df_clean.groupBy("origin_country") \
    .agg(count("*").alias("total_calls")) \
    .orderBy("total_calls", ascending=False) \
    .write.jdbc(url=pg_url, table="gold.kpi_total_calls_by_origin_country", mode="overwrite", properties=pg_properties)

# KPI 2
df_clean.groupBy("destination_country") \
    .agg(
        count(when(col("call_status") == "Answered", True)).alias("answered_calls"),
        count("*").alias("total_calls")
    ) \
    .withColumn("answer_rate_pct", expr("ROUND(100.0 * answered_calls / total_calls, 2)")) \
    .orderBy("answer_rate_pct", ascending=False) \
    .write.jdbc(url=pg_url, table="gold.kpi_answer_rate_by_destination", mode="overwrite", properties=pg_properties)

# KPI 3
df_clean.withColumn("operator_prefix", substring("destination_country", -3, 3)) \
    .filter(col("call_status") == "Answered") \
    .groupBy("operator_prefix") \
    .agg(avg("call_duration_seconds").alias("avg_duration_secs")) \
    .orderBy("avg_duration_secs", ascending=False) \
    .write.jdbc(url=pg_url, table="gold.kpi_avg_duration_by_prefix", mode="overwrite", properties=pg_properties)

# KPI 4
df_clean.filter(col("call_status") == "Unanswered") \
    .groupBy("release_cause") \
    .agg(count("*").alias("failures")) \
    .orderBy("failures", ascending=False) \
    .write.jdbc(url=pg_url, table="gold.kpi_release_cause_summary", mode="overwrite", properties=pg_properties)

# KPI 5
df_clean.filter(col("call_status") == "Unanswered") \
    .groupBy("a_number") \
    .agg(count("*").alias("failed_calls")) \
    .filter(col("failed_calls") > 3) \
    .orderBy("failed_calls", ascending=False) \
    .write.jdbc(url=pg_url, table="gold.kpi_failed_callers", mode="overwrite", properties=pg_properties)

# KPI 6
df_clean.withColumn("call_hour", hour("call_time")) \
    .groupBy("call_hour") \
    .agg(count("*").alias("total_calls")) \
    .orderBy("call_hour") \
    .write.jdbc(url=pg_url, table="gold.kpi_hourly_call_volume", mode="overwrite", properties=pg_properties)

spark.stop()
