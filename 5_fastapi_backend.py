from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
import faiss
import pandas as pd
import numpy as np
import uuid
from confluent_kafka import Producer
import time
import lightgbm as lgb

app = FastAPI(title="StyleStream E-Commerce API")
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# --- 1. LOAD DATABASES & MAPPINGS ---
print("Loading FAISS Visual Index and User Mappings...")
try:
    visual_index = faiss.read_index("hm_visual_index.faiss")
    faiss_mapping = pd.read_csv("faiss_mapping.csv")
    
    # Load user mapping so we can log in as historical H&M users!
    user_mapping = pd.read_csv("user_mapping.csv")
    print(f"Loaded {len(user_mapping)} H&M users for simulation.")
    try:
        lgb_ranker = lgb.Booster(model_file="lgb_model.txt")
        print("Successfully loaded LightGBM Ranker!")
    except Exception as e:
        lgb_ranker = None
        print(f"Warning: LightGBM model 'lgb_model.txt' not found. Will default to FAISS scoring. ({e})")
        
except Exception as e:
    print(f"Critical Error loading databases: {e}")
except Exception as e:
    print(f"Warning: Could not load index or mappings. Error: {e}")

# --- 2. AUTHENTICATION SCHEMAS ---
class LoginRequest(BaseModel):
    username: str
    password: str

# --- 3. API ENDPOINTS ---

@app.post("/login")
def login(req: LoginRequest):
    """Handles both H&M historical users and newly signed-up users."""
    
    # Check if this is an H&M Historical User (e.g., username is a number like '42')
    if req.username.isdigit():
        user_int = int(req.username)
        # Check if they exist in the Kaggle dataset
        if user_int in user_mapping['user_id_int'].values:
            if req.password == "password123":
                # Success! Fetch their real Kaggle ID
                kaggle_id = user_mapping.loc[user_mapping['user_id_int'] == user_int, 'customer_id'].values[0]
                return {"status": "success", "user_type": "historical", "kaggle_id": kaggle_id, "username": req.username}
            else:
                raise HTTPException(status_code=401, detail="Invalid password for H&M user. Use password123.")
    
    # If not a number, check if it's a new user stored in Redis
    user_data = r.get(f"auth:{req.username}")
    if user_data:
        parsed_data = json.loads(user_data)
        if parsed_data["password"] == req.password:
            return {"status": "success", "user_type": "new", "kaggle_id": parsed_data["kaggle_id"], "username": req.username}
        else:
            raise HTTPException(status_code=401, detail="Invalid password.")
            
    raise HTTPException(status_code=404, detail="User not found.")

@app.post("/signup")
def signup(req: LoginRequest):
    """Creates a new user profile from scratch."""
    if r.exists(f"auth:{req.username}") or req.username.isdigit():
        raise HTTPException(status_code=400, detail="Username already exists or is reserved for H&M users.")
        
    # Generate a brand new unique ID for this user
    new_kaggle_id = f"new_user_{uuid.uuid4().hex}"
    
    # Save to Redis
    r.set(f"auth:{req.username}", json.dumps({
        "password": req.password,
        "kaggle_id": new_kaggle_id
    }))
    
    return {"status": "success", "message": "Account created!", "kaggle_id": new_kaggle_id}

# --- KAFKA PRODUCER (Real Telemetry) ---
conf = {'bootstrap.servers': '127.0.0.1:9092', 'client.id': 'fastapi-backend'}
producer = Producer(conf)

class Interaction(BaseModel):
    user_id: str
    item_id: str
    action: str

@app.post("/interact")
def record_interaction(interaction: Interaction):
    """Takes a real click/purchase from the website and injects it into Kafka."""
    try:
        event = {
            "user_id": interaction.user_id,
            "item_id": interaction.item_id,
            "action": interaction.action,
            "timestamp": time.time()
        }
        # Push to the live Kafka stream!
        producer.produce('user-clicks', value=json.dumps(event).encode('utf-8'))
        producer.poll(0) 
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/trending")
def get_trending_items():
    # (Keep your existing trending code here)
    try:
        trending_data = r.get("global_trending")
        if trending_data:
            return {"type": "trending", "items": json.loads(trending_data)}
        return {"type": "fallback", "items": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/similar/{article_id}")
def get_hybrid_similar_items(article_id: str, username: str = ""):
    """Two-Stage Hybrid: FAISS Visual Candidates + LightGBM Historical Ranking"""
    try:
        # --- STAGE 1: CANDIDATE GENERATION (FAISS) ---
        row_indices = faiss_mapping.index[faiss_mapping['article_id'] == int(article_id)].tolist()
        if not row_indices:
             return {"type": "hybrid_similarity", "items": []}
             
        row_idx = row_indices[0]
        vector = visual_index.reconstruct(row_idx)
        
        # Fetch 20 visual candidates
        distances, indices = visual_index.search(np.array([vector]), 20) 
        similar_indices = indices[0][1:]
        visual_candidates_int = faiss_mapping.iloc[similar_indices]['article_id'].tolist()
        
        # --- STAGE 2: THE HISTORICAL RANKER (LightGBM) ---
        final_ranked_items = visual_candidates_int[:5] # Default fallback
        
        if lgb_ranker and username.isdigit():
            user_int = int(username)
            # Fetch the user's real Kaggle ID
            if user_int in user_mapping['user_id_int'].values:
                kaggle_id = user_mapping.loc[user_mapping['user_id_int'] == user_int, 'customer_id'].values[0]
                
                # Fetch their ALS taste profile from Redis
                user_vector_json = r.get(f"user:{kaggle_id}")
                
                if user_vector_json:
                    user_vector = json.loads(user_vector_json)
                    
                    # 1. Construct the Feature Matrix for LightGBM
                    # (This matches the exact 2D array format LightGBM expects)
                    features = []
                    for candidate_id in visual_candidates_int:
                        # Combine: [Item ID, User ID, User Vector Features...]
                        # Note: In production, you would append the Item's ALS vector here too!
                        row_features = [candidate_id, user_int] + user_vector 
                        features.append(row_features)
                    
                    feature_matrix = np.array(features)
                    
                    # 2. Predict the Probability of Purchase!
                    scores = lgb_ranker.predict(feature_matrix)
                    
                    # 3. Sort the 20 candidates by highest LightGBM score
                    # np.argsort sorts lowest to highest, so we reverse it with [::-1]
                    best_indices = np.argsort(scores)[::-1]
                    final_ranked_items = [visual_candidates_int[i] for i in best_indices[:5]]

        # Format back to 10-digit strings for the UI images
        final_ranked_items = [str(s).zfill(10) for s in final_ranked_items]
        
        return {"type": "hybrid_similarity", "items": final_ranked_items}
        
    except Exception as e:
        print(f"Server Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))