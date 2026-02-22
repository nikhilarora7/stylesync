from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
import faiss
import pandas as pd
import numpy as np
import uuid

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
def get_similar_items(article_id: str):
    # (Keep your existing FAISS visual similarity code here)
    try:
        row_indices = faiss_mapping.index[faiss_mapping['article_id'] == int(article_id)].tolist()
        if not row_indices:
             return {"type": "visual_similarity", "items": []}
        row_idx = row_indices[0]
        vector = visual_index.reconstruct(row_idx)
        distances, indices = visual_index.search(np.array([vector]), 6)
        similar_indices = indices[0][1:]
        similar_ids = faiss_mapping.iloc[similar_indices]['article_id'].astype(str).tolist()
        similar_ids = [s.zfill(10) for s in similar_ids]
        return {"type": "visual_similarity", "items": similar_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))