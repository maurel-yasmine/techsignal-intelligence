from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("smoke").getOrCreate()
spark.range(5).selectExpr("id", "id * 2 AS double_id").write.mode("overwrite").parquet("file:///D:/PROJECT/tmp_smoke/parquet")
print("SMOKE OK")
spark.stop()