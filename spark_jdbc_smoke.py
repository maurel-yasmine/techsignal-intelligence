from pyspark.sql import SparkSession
import os

PG_HOST = "aws-0-ap-southeast-1.pooler.supabase.com"   
# Menggunakan endpoint sesuai nama organisasimu di Supabase (maurel-yasmine)
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:5432/postgres?options=endpoint%3Dusapduupljixqwcystla"

PROPS = {    
    "driver": "org.postgresql.Driver",    
    "sslmode": "require",    
    "user": "postgres.usapduupljixqwcystla", 
    "password": "5Up@bas3pmg",
    "prepareThreshold": "0",
}

JAR = "file:///D:/PROJECT/jars/postgresql-42.7.4.jar"

spark = (    
    SparkSession.builder    
    .master("local[*]")    
    .appName("pg_jdbc_smoke")    
    .config("spark.jars", JAR)    
    .getOrCreate()
)

table = (    
    "(SELECT run_id::text AS run_id, trigger_type, status, "    
    "stories_loaded, comments_loaded, trends_loaded, "    
    "started_at, completed_at FROM ops.collection_run) t"
)

df = spark.read.jdbc(url=JDBC_URL, table=table, properties=PROPS)

print("rows in ops.collection_run:", df.count())
df.printSchema()
df.show(truncate=50)

spark.stop()