import pandas as pd
import redis
import json
import ast

# Connect to the local Redis container
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

print("Connecting to Redis and clearing old data...")
r.flushdb() # Clears out any old data for a fresh start

# Load the Kaggle outputs you downloaded
print("Loading CSV files...")
df_vectors = pd.read_csv("user_vectors.csv")
df_mapping = pd.read_csv("user_mapping.csv")

# Merge them so we have the real 64-character Kaggle ID next to the vector math
df_merged = pd.merge(df_mapping, df_vectors, left_on="user_id_int", right_on="id")

print(f"Pushing {len(df_merged)} user vectors to Redis...")

# Loop through and save to Redis
for index, row in df_merged.iterrows():
    kaggle_id = row['customer_id']
    
    # Spark saves vectors as a string representing a list (e.g., "[0.1, 0.2]"). 
    # We use ast.literal_eval to safely convert it back to a Python list.
    vector_list = ast.literal_eval(row['features']) 
    
    # Store in Redis. Key = "user:{kaggle_id}", Value = JSON string of the vector
    redis_key = f"user:{kaggle_id}"
    r.set(redis_key, json.dumps(vector_list))

print("Successfully loaded all User Vectors into Redis!")
# Test retrieval
sample_key = r.randomkey()
print(f"Test Retrieval for {sample_key}: {r.get(sample_key)[:50]}...")