from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, substring, avg, count, expr

# 1. Start Spark Session
spark = SparkSession.builder \
    .appName("CDR Analytics Gold Layer") \
    .config("spark.jars", r"C:\Users\arjun\Downloads\postgresql-42.2.5.jar") \
    .getOrCreate()

# 2. PostgreSQL connection details
pg_url = "jdbc:postgresql://localhost:5432/CDR_project"  # Update your DB name
pg_properties = {
    "user": "postgres",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

# 3. Load the raw CDR CSV
df_raw = spark.read.csv(
    r"C:\Users\arjun\Downloads\cdr_og1.csv",
    header=True,
    inferSchema=True
)

# 4. Clean and prepare Gold Layer
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

# 5. Write Gold Layer to PostgreSQL
df_clean.write.jdbc(url=pg_url, table="gold.cdr_cleaned", mode="overwrite", properties=pg_properties)

# KPI 1: Call Volume by Origin Country
kpi1 = df_clean.groupBy("origin_country") \
    .agg(count("*").alias("total_calls")) \
    .orderBy("total_calls", ascending=False)

kpi1.write.jdbc(url=pg_url, table="gold.kpi_total_calls_by_origin_country", mode="overwrite", properties=pg_properties)

# KPI 2: Answer Rate by Destination
kpi2 = df_clean.groupBy("destination_country") \
    .agg(
        count(when(col("call_status") == "Answered", True)).alias("answered_calls"),
        count("*").alias("total_calls")
    ) \
    .withColumn("answer_rate_pct", expr("ROUND(100.0 * answered_calls / total_calls, 2)")) \
    .orderBy("answer_rate_pct", ascending=False)

kpi2.write.jdbc(url=pg_url, table="gold.kpi_answer_rate_by_destination", mode="overwrite", properties=pg_properties)

# KPI 3: Avg Duration by Destination Prefix
kpi3 = df_clean \
    .withColumn("operator_prefix", substring("destination_country", -3, 3)) \
    .filter(col("call_status") == "Answered") \
    .groupBy("operator_prefix") \
    .agg(avg("call_duration_seconds").alias("avg_duration_secs")) \
    .orderBy("avg_duration_secs", ascending=False)

kpi3.write.jdbc(url=pg_url, table="gold.kpi_avg_duration_by_prefix", mode="overwrite", properties=pg_properties)

# KPI 4: Top Release Causes
kpi4 = df_clean.filter(col("call_status") == "Unanswered") \
    .groupBy("release_cause") \
    .agg(count("*").alias("failures")) \
    .orderBy("failures", ascending=False)

kpi4.write.jdbc(url=pg_url, table="gold.kpi_release_cause_summary", mode="overwrite", properties=pg_properties)

# KPI 5: Frequent Failed Callers
kpi5 = df_clean.filter(col("call_status") == "Unanswered") \
    .groupBy("a_number") \
    .agg(count("*").alias("failed_calls")) \
    .filter(col("failed_calls") > 3) \
    .orderBy("failed_calls", ascending=False)

kpi5.write.jdbc(url=pg_url, table="gold.kpi_failed_callers", mode="overwrite", properties=pg_properties)

# KPI 6: Hourly Call Volume
kpi6 = df_clean \
    .withColumn("call_hour", hour("call_time")) \
    .groupBy("call_hour") \
    .agg(count("*").alias("total_calls")) \
    .orderBy("call_hour")

kpi6.write.jdbc(url=pg_url, table="gold.kpi_hourly_call_volume", mode="overwrite", properties=pg_properties)


# Stop Spark
spark.stop()
