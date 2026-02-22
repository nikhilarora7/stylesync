import streamlit as st
import requests
import os
from PIL import Image

# --- 1. CONFIGURATION ---
API_BASE_URL = "http://127.0.0.1:8000"
# UPDATE THIS PATH to where your H&M images folder is located
IMAGE_DIR = r"D:\entahhhtainmentttttt\h-and-m-personalized-fashion-recommendations\images" 

st.set_page_config(page_title="StyleStream AI", layout="wide")

# --- 2. HELPER FUNCTIONS ---
def get_image_path(article_id):
    """H&M image files are 10 digits long, padded with zeros, stored in subfolders."""
    article_str = str(article_id).zfill(10)
    subfolder = article_str[:3]
    filename = f"{article_str}.jpg"
    return os.path.join(IMAGE_DIR, subfolder, filename)

def fetch_trending():
    """Calls our FastAPI backend to get the real-time trending list."""
    try:
        response = requests.get(f"{API_BASE_URL}/trending")
        if response.status_code == 200:
            return response.json().get("items", [])
    except:
        st.error("Could not connect to FastAPI Backend. Is it running?")
    return []

def fetch_user_data(username):
    """Calls FastAPI to log the user in and get their profile."""
    try:
        response = requests.get(f"{API_BASE_URL}/user/{username}")
        if response.status_code == 200:
            return response.json()
    except:
        return None
    return None

# --- 3. THE UI: SIDEBAR ---
st.sidebar.title("StyleStream Control Panel")
st.sidebar.markdown("Simulate different user states below:")

# The Mock Login Dropdown
selected_user = st.sidebar.selectbox(
    "Select User State",
    ["new_user", "streetwear_fan", "formal_fan"]
)

# Fetch user data based on selection
user_data = fetch_user_data(selected_user)
if user_data:
    if user_data["status"] == "new_user":
        st.sidebar.warning("Cold Start: Showing global trending items.")
    elif user_data["status"] == "returning_user":
        st.sidebar.success(f"Warm Start: Vector loaded for {user_data['kaggle_id'][:8]}...")
        st.sidebar.write("ALS Vector Preview:", user_data["vector_preview"])

# --- 4. THE UI: MAIN FEED WITH SESSION STATE ---
st.title("StyleStream | Live Feed")
st.markdown("---")

# Initialize Session State (Streamlit's memory)
if 'current_feed_items' not in st.session_state:
    st.session_state.current_feed_items = fetch_trending()
if 'feed_title' not in st.session_state:
    st.session_state.feed_title = "🔥 Global Trending Right Now"

st.subheader(st.session_state.feed_title)

if not st.session_state.current_feed_items:
    st.info("Waiting for data... Keep the Kafka Producer and Spark Consumer running!")
else:
    cols = st.columns(5)
    
    for idx, article_id in enumerate(st.session_state.current_feed_items):
        col = cols[idx % 5] 
        
        with col:
            img_path = get_image_path(article_id)
            try:
                img = Image.open(img_path)
                st.image(img, use_column_width=True)
                
                # THE MAGIC BUTTON
                if st.button(f"Find Similar", key=f"btn_{article_id}_{idx}"):
                    # When clicked, call the new FastAPI endpoint
                    response = requests.get(f"{API_BASE_URL}/similar/{article_id}")
                    if response.status_code == 200:
                        # Update the screen with the visually similar items!
                        st.session_state.current_feed_items = response.json().get("items", [])
                        st.session_state.feed_title = "👁️ Visually Similar Items"
                        st.rerun() # Force the page to refresh instantly
                    else:
                        st.error("No visual matches found for this item.")
                        
            except FileNotFoundError:
                st.info(f"Image Missing\nID: {article_id}")
                
# Add a reset button to go back to real-time trending
st.markdown("---")
if st.button("⬅️ Back to Live Trending"):
    st.session_state.current_feed_items = fetch_trending()
    st.session_state.feed_title = "🔥 Global Trending Right Now"
    st.rerun()