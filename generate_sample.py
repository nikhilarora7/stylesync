import pandas as pd
import random

print("Loading user and item lists...")
users = pd.read_csv("user_mapping.csv")['customer_id'].tolist()

# FIX: Only pick items that exist in our FAISS Visual Index!
items = pd.read_csv("faiss_mapping.csv")['article_id'].tolist()

print("Generating 5,000 random simulated clicks...")
sample_data = {
    "customer_id": [random.choice(users) for _ in range(5000)],
    "article_id": [random.choice(items) for _ in range(5000)]
}

df = pd.DataFrame(sample_data)
df.to_csv("transactions_sample.csv", index=False)
print("Successfully created transactions_sample.csv!")