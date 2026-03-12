from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
import ast
import redis
import json
import faiss
import pandas as pd
import numpy as np
import uuid
import time
import sqlite3
import lightgbm as lgb
import os

app = FastAPI(title="StyleStream E-Commerce API")

# --- 1. CONFIGURATION & DATABASE CONNECTIONS ---
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
conf = {'bootstrap.servers': '127.0.0.1:9092', 'client.id': 'fastapi-backend'}
producer = Producer(conf)

def get_db_connection():
    # check_same_thread=False allows FastAPI to use multiple worker threads safely
    conn = sqlite3.connect('auth.db', check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# --- 2. LOAD MODELS & MAPPINGS ---
print("Loading Engine Components...")
try:
    # Load Visual Search
    visual_index = faiss.read_index("hm_visual_index.faiss")
    faiss_mapping = pd.read_csv("faiss_mapping.csv")
    
    # Load LightGBM Ranker
    if os.path.exists("lgb_model.txt"):
        lgb_ranker = lgb.Booster(model_file="lgb_model.txt")
        print("✅ LightGBM Ranker Loaded!")
    else:
        lgb_ranker = None
        print("⚠️ LightGBM model not found. Using fallback logic.")
# --- NEW: LOAD STATIC ALS VECTORS INTO RAM ---
    try:
        df_vectors = pd.read_csv("user_vectors.csv")
        # Convert the string representation of the list back into a real Python list
        df_vectors['features'] = df_vectors['features'].apply(ast.literal_eval)
        # Create a blazing fast dictionary lookup: user_int -> feature_list
        user_vector_dict = dict(zip(df_vectors['id'], df_vectors['features']))
        print(f"✅ Loaded ALS vectors for {len(user_vector_dict)} users directly into RAM.")
    except Exception as e:
        user_vector_dict = {}
        print(f"⚠️ Warning: user_vectors.csv not found. {e}")
        

except Exception as e:
    print(f"🔥 Critical Error loading models: {e}")

# --- NEW: SYSTEM INITIALIZATION (The Cold Start Fix) ---
print("Running System Cold-Start Checks...")

# 1. Force Kafka Topic Creation
# Sending a single dummy message forces Kafka to auto-create 'live-clicks'
try:
    dummy_event = {
        "user_id": "system_boot",
        "item_id": "0000000000",
        "action": "boot",
        "timestamp": time.time()
    }
    producer.produce('live-clicks', value=json.dumps(dummy_event).encode('utf-8'))
    producer.flush() # Ensure it sends immediately
    print("✅ Kafka 'live-clicks' channel ready.")
except Exception as e:
    print(f"⚠️ Kafka boot warning: {e}")

# 2. Seed Redis so the UI is never blank
if not r.exists("global_trending"):
    # We inject 5 hardcoded valid H&M IDs so you have something to click!
    # (Using random popular items from the FAISS mapping)
    seed_items = ["0108775015", "0108775044", "0111565001", "0111586001", "0111593001"]
    r.set("global_trending", json.dumps(seed_items))
    print("✅ Seeded Redis with default trending items.")

# --- 3. DATA MODELS ---
class LoginRequest(BaseModel):
    username: str
    password: str

class Interaction(BaseModel):
    user_id: str
    username: str # Added username to track session history
    item_id: str
    action: str

# --- 4. AUTH ENDPOINTS ---
@app.post("/login")
def login(req: LoginRequest):
    # 'with' automatically closes the connection even if code crashes!
    with get_db_connection() as conn:
        user = conn.execute('SELECT * FROM users WHERE username = ?', (req.username,)).fetchone()
    
    if user and user['password'] == req.password:
        return {
            "status": "success", 
            "user_type": user['user_type'], 
            "kaggle_id": user['kaggle_id'], 
            "username": user['username']
        }
    raise HTTPException(status_code=401, detail="Invalid credentials.")

@app.post("/signup")
def signup(req: LoginRequest):
    with get_db_connection() as conn:
        user = conn.execute('SELECT * FROM users WHERE username = ?', (req.username,)).fetchone()
        
        if user:
            raise HTTPException(status_code=400, detail="Username taken.")
            
        new_kaggle_id = f"new_user_{uuid.uuid4().hex}"
        try:
            conn.execute(
                'INSERT INTO users (username, password, kaggle_id, user_type) VALUES (?, ?, ?, ?)',
                (req.username, req.password, new_kaggle_id, "new")
            )
            conn.commit()
        except:
            raise HTTPException(status_code=400, detail="Error creating account.")
            
    return {"status": "success", "kaggle_id": new_kaggle_id}

# --- 5. TELEMETRY & TRACKING ---
@app.post("/interact")
def record_interaction(interaction: Interaction):
    """
    1. Sends event to Kafka (for Global Trending)
    2. Saves event to Redis (for Real-time User Profile)
    """
    try:
        # --- NEW: Print to terminal so we know FastAPI got it! ---
        print(f"🎯 UI Click Received: [{interaction.action}] on Item {interaction.item_id}")

        # A. Send to Kafka
        event = {
            "user_id": interaction.user_id,
            "item_id": interaction.item_id,
            "action": interaction.action,
            "timestamp": time.time()
        }
        producer.produce('live-clicks', value=json.dumps(event).encode('utf-8'))
        
        # --- THE FIX: Force the buffer to send to Kafka INSTANTLY ---
        producer.flush() 
        
        # B. Save to Redis Session History
        redis_key = f"history:{interaction.username}"
        r.lpush(redis_key, interaction.item_id)
        r.ltrim(redis_key, 0, 19)
        
        return {"status": "success"}
    except Exception as e:
        print(f"❌ Interaction Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- 6. RECOMMENDATION ENDPOINTS ---

@app.get("/feed/{username}")
def get_personalized_feed(username: str):
    """
    THE MASTER BLENDER: 2 Historical (ALS+LightGBM) + 3 Viral (PySpark)
    """
    try:
        # 1. Fetch Global Trending from PySpark (The Viral Pool)
        trending_data = r.get("global_trending")
        trending_items = json.loads(trending_data) if trending_data else []
        
        # 2. Safely Parse the Username (Handling Kaggle's .0 floats)
        try:
            user_int = int(float(username))
            is_historical = True
        except ValueError:
            is_historical = False
            
        # 3. Handle NEW USERS (100% Viral / Cold Start)
        if not is_historical or not lgb_ranker or user_int not in user_vector_dict:
            return {"type": "global_trending", "items": trending_items[:5]}
            
        # 4. Handle HISTORICAL USERS (The 2/3 Blend)
        user_vector = user_vector_dict[user_int]
        
        # --- SLOTS 1 & 2: PERSONALIZED RANKING (ALS + LightGBM) ---
        import random
        valid_items = faiss_mapping['article_id'].dropna().unique().tolist()
        candidate_pool = random.sample(valid_items, min(100, len(valid_items)))
        
        features = []
        valid_candidates = []
        for item_int in candidate_pool:
            try:
                row = [item_int, user_int] + user_vector 
                features.append(row)
                valid_candidates.append(str(item_int).zfill(10))
            except:
                continue
                
        # Let LightGBM evaluate the candidate pool
        scores = lgb_ranker.predict(np.array(features))
        best_indices = np.argsort(scores)[::-1] 
        
        als_recs = []
        for idx in best_indices:
            item_str = valid_candidates[idx]
            if item_str not in als_recs:
                als_recs.append(item_str)
            if len(als_recs) == 2: # EXACTLY 2 PERSONALIZED ITEMS
                break

        # --- SLOTS 3, 4 & 5: LIVE TRENDING (PySpark) ---
        trending_recs = []
        for t in trending_items:
            # Prevent showing the same item twice if it's both personalized AND trending
            if t not in als_recs: 
                trending_recs.append(t)
            if len(trending_recs) == 3: # EXACTLY 3 VIRAL ITEMS
                break
                
        # --- THE FINAL BLEND ---
        final_feed = als_recs + trending_recs
        
        # Safety Fallback: Guarantee exactly 5 images render if pools were empty
        if len(final_feed) < 5:
            for t in valid_items:
                t_str = str(t).zfill(10)
                if t_str not in final_feed:
                    final_feed.append(t_str)
                if len(final_feed) == 5:
                    break
                    
        return {"type": "blended_feed", "items": final_feed}

    except Exception as e:
        print(f"Feed Error: {e}")
        return {"type": "error", "items": []}

@app.get("/similar/{article_id}")
def get_similar_items(article_id: str):
    """
    Visual Similarity Endpoint (Strictly Visual as requested)
    """
    try:
        row_indices = faiss_mapping.index[faiss_mapping['article_id'] == int(article_id)].tolist()
        if not row_indices:
             return {"type": "visual", "items": []}
        
        row_idx = row_indices[0]
        vector = visual_index.reconstruct(row_idx)
        distances, indices = visual_index.search(np.array([vector]), 6)
        
        similar_ids = faiss_mapping.iloc[indices[0][1:]]['article_id'].astype(str).tolist()
        similar_ids = [s.zfill(10) for s in similar_ids]
        
        return {"type": "visual", "items": similar_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))