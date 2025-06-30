from pyspark.sql import SparkSession

def get_spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", r"C:\Users\arjun\Downloads\postgresql-42.2.5.jar") \
        .getOrCreate()

# PostgreSQL connection URL
pg_url = "jdbc:postgresql://localhost:5432/CDR_project"

# PostgreSQL properties
pg_properties = {
    "user": "postgres",
    "password": "root",
    "driver": "org.postgresql.Driver"
}
