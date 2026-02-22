from fastapi import FastAPI, HTTPException
import redis
import json
import faiss
import pandas as pd
import numpy as np
# --- 1. INITIALIZATION ---
app = FastAPI(title="StyleStream Recommendation API")

# Connect to our local Redis feature store
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
# --- NEW: LOAD THE VISUAL BRAIN ---
print("Loading FAISS Visual Index...")
try:
    visual_index = faiss.read_index("hm_visual_index.faiss")
    faiss_mapping = pd.read_csv("faiss_mapping.csv")
    print(f"Loaded {visual_index.ntotal} image vectors!")
except Exception as e:
    print(f"Warning: Could not load FAISS index. Error: {e}")
# --- 2. THE MOCK LOGIN DICTIONARY ---
# We map simple usernames to the massive 64-character Kaggle IDs
# (Note: I am using random Kaggle IDs here, we will pull real ones later)
DEMO_USERS = {
    "new_user": None, # Represents a cold start
    "streetwear_fan": "000058a12d5b43e67d225668fa1f8d618c13dc232df0cad8ffe7ad4a10a1e30",
    "formal_fan": "00007d2de826758b65a93dd24ce629ed66842531df6699338c5570910a014cc"
}

# --- 3. API ENDPOINTS ---

@app.get("/")
def health_check():
    return {"status": "StyleStream API is running!"}

@app.get("/trending")
def get_trending_items():
    try:
        trending_data = r.get("global_trending")
        if trending_data:
            return {"type": "trending", "items": json.loads(trending_data)}
        return {"type": "fallback", "items": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# --- NEW: THE VISUAL SIMILARITY ENDPOINT ---
@app.get("/similar/{article_id}")
def get_similar_items(article_id: str):
    """Finds visually similar items using the FAISS index."""
    try:
        row_indices = faiss_mapping.index[faiss_mapping['article_id'] == int(article_id)].tolist()
        
        # FIX: Gracefully return empty if the item isn't in our database
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
        print(f"Server Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/user/{username}")
def get_user_profile(username: str):
    """Logs the user in and fetches their collaborative filtering vector."""
    if username not in DEMO_USERS:
        raise HTTPException(status_code=404, detail="User not found in demo database.")
        
    kaggle_id = DEMO_USERS[username]
    
    # Handle New Users (Cold Start)
    if kaggle_id is None:
        return {"status": "new_user", "message": "Route to /trending endpoint"}
        
    # Handle Returning Users (Warm Start)
    try:
        vector_data = r.get(f"user:{kaggle_id}")
        if vector_data:
             # Just returning the first 5 numbers of the vector to prove it works
            vector_list = json.loads(vector_data)
            return {
                "status": "returning_user", 
                "kaggle_id": kaggle_id,
                "vector_preview": vector_list[:5] 
            }
        else:
            return {"status": "no_history", "message": "User exists but has no ALS vector."}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))