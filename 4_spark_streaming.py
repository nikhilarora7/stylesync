import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ['HADOOP_HOME'] + '\\bin;' + os.environ.get('PATH', '')

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import redis
import json

spark_version = pyspark.__version__
os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version} pyspark-shell'

print("Starting StyleStream Real-Time Engine (Clean Stream)...")
spark = SparkSession.builder \
    .appName("StyleStream-Live-Trending") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("item_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

# --- CONNECT TO THE NEW KAFKA CHANNEL ---
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "live-clicks") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# --- SCORING: view=1, cart=3, purchase=10 ---
# (And ignore the dummy system boot event!)
scored_df = parsed_df \
    .filter(col("item_id") != "0000000000") \
    .withColumn(
        "score",
        when(col("action") == "purchase", 10)
        .when(col("action") == "add_to_cart", 3)
        .otherwise(1)
    )

# Global Aggregation (Tracks all clicks since the script started)
agg_df = scored_df.groupBy("item_id").agg(sum("score").alias("total_score"))

# --- WRITE TO REDIS ---
def update_redis(batch_df, batch_id):
    # Sort by highest score and take the Top 10
    top_items_df = batch_df.orderBy(col("total_score").desc()).limit(10)
    top_items = [row['item_id'] for row in top_items_df.collect()]
    
    if top_items:
        print(f"[Batch {batch_id}] Live Leaderboard: {top_items}")
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.set("global_trending", json.dumps(top_items))

# --- START STREAM ---
query = agg_df.writeStream \
    .outputMode("complete") \
    .option("checkpointLocation", "./spark_checkpoints_live") \
    .foreachBatch(update_redis) \
    .start()

query.awaitTermination()