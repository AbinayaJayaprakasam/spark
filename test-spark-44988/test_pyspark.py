from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test SPARK-44988") \
    .master("local[*]") \
    .getOrCreate()

try:
    print("Attempting to read Parquet file with TIMESTAMP(NANOS,false)...")
    df = spark.read.parquet("test_nanos_timestamp.parquet")
    df.printSchema()
    df.show()
    print("SUCCESS: File read without error!")
except Exception as e:
    print(f"ERROR: {type(e).__name__}")
    print(f"Message: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
