import os
import pyspark

spark_version = pyspark.__version__

os.environ['HADOOP_HOME'] = 'C:\\hadoop'
# --- ADD THIS LINE TO FIX THE NATIVE IO DLL ERROR ---
os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ.get('PATH', '')

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version} pyspark-shell'
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import redis
import json

# --- 1. INITIALIZE SPARK STREAMING ---
spark = SparkSession.builder \
    .appName("StyleStream-Trending") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Hide the massive walls of Spark warning logs
spark.sparkContext.setLogLevel("ERROR")
print("Spark Streaming Session Started! Listening to Kafka for clicks...")

# --- 2. DEFINE THE INCOMING DATA SCHEMA ---
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

# --- 3. CONNECT TO KAFKA ---
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "user-clicks") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- 4. PARSE THE JSON EVENTS ---
# Kafka sends data as raw binary bytes. We convert it to strings, then to JSON columns.
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# --- 5. THE REDIS UPDATE LOGIC (Micro-Batching) ---
def update_trending_in_redis(batch_df, batch_id):
    """This function runs every few seconds on the newest chunk of clicks."""
    if not batch_df.isEmpty():
        # Count clicks per item and get the Top 10
        top_items_df = batch_df.groupBy("item_id").count().orderBy(col("count").desc()).limit(10)
        
        # Convert the Spark DataFrame to a normal Python list
        top_items = [row['item_id'] for row in top_items_df.collect()]
        
        if top_items:
            # Connect to Redis and overwrite the 'global_trending' key
            try:
                r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
                r.set("global_trending", json.dumps(top_items))
                print(f"[Batch {batch_id}] Updated Redis with {len(top_items)} trending items: {top_items[:3]}...")
            except Exception as e:
                print(f"Redis connection error: {e}")

# --- 6. START THE STREAM ---
query = parsed_df.writeStream \
    .outputMode("update") \
    .option("checkpointLocation", "./spark_checkpoints") \
    .foreachBatch(update_trending_in_redis) \
    .start()

# Keep the script running forever
query.awaitTermination()