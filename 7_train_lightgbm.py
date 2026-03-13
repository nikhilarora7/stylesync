import pandas as pd
import lightgbm as lgb
import random
import ast

# --- 1. LOAD THE DATA ---
print("Loading databases...")
df_transactions = pd.read_csv("transactions_sample.csv")
df_mapping = pd.read_csv("user_mapping.csv")
df_vectors = pd.read_csv("user_vectors.csv")
df_items = pd.read_csv("faiss_mapping.csv")

# --- 2. BUILD FAST LOOKUP DICTIONARIES ---
print("Building memory lookups...")
user_map_dict = dict(zip(df_mapping['customer_id'], df_mapping['user_id_int']))
# Remember PySpark renames the user_id column to 'id'
user_vector_dict = dict(zip(df_vectors['id'], df_vectors['features'].apply(ast.literal_eval)))
valid_items = df_items['article_id'].tolist()

# --- 3. GENERATE POSITIVE & NEGATIVE SAMPLES ---
print("Generating training dataset...")
training_data = []

for _, row in df_transactions.iterrows():
    customer_id = row['customer_id']
    article_id = int(row['article_id'])
    
    # Safely skip if data is missing
    if customer_id not in user_map_dict: continue
    user_int = user_map_dict[customer_id]
    if user_int not in user_vector_dict: continue
        
    user_vector = user_vector_dict[user_int]
    
    # [POSITIVE SAMPLE]: The user actually bought this item (Label = 1)
    # Format matches the FastAPI backend: [item_id, user_id, user_features...]
    training_data.append([article_id, user_int] + user_vector + [1])
    
    # [NEGATIVE SAMPLE]: The user did NOT buy this random item (Label = 0)
    negative_item = random.choice(valid_items)
    training_data.append([negative_item, user_int] + user_vector + [0])

# --- 4. PREPARE LIGHTGBM DATASET ---
# Create dynamic column names based on the size of your ALS vector
cols = ['item_id', 'user_id'] + [f'uf_{i}' for i in range(len(user_vector))] + ['label']
df_train = pd.DataFrame(training_data, columns=cols)

X = df_train.drop('label', axis=1)
y = df_train['label']

print(f"Created dataset with {len(df_train)} rows. Training LightGBM...")

# We explicitly tell LightGBM that item_id and user_id are categories, not continuous numbers!
train_data = lgb.Dataset(X, label=y, categorical_feature=['item_id', 'user_id'])

# --- 5. TRAIN AND SAVE ---
params = {
    'objective': 'binary',        # We are predicting a Yes(1) or No(0)
    'metric': 'binary_logloss',
    'boosting_type': 'gbdt',
    'learning_rate': 0.1,
    'num_leaves': 31,
    'verbose': -1
}

model = lgb.train(params, train_data, num_boost_round=100)

# Save the model so FastAPI can load it!
model.save_model("lgb_model.txt")
print("Successfully trained and saved lgb_model.txt!")