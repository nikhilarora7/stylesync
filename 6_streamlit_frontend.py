import streamlit as st
import requests
import os
from PIL import Image

# --- CONFIGURATION ---
API_BASE_URL = "http://127.0.0.1:8000"
IMAGE_DIR = r"D:\entahhhtainmentttttt\h-and-m-personalized-fashion-recommendations\images" # Ensure this points to your H&M images

st.set_page_config(page_title="StyleStream AI", layout="wide")

# --- SESSION STATE ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.session_state.kaggle_id = ""
    st.session_state.user_type = ""
if 'cart' not in st.session_state:
    st.session_state.cart = []
if 'checkout_success' not in st.session_state:
    st.session_state.checkout_success = False

# --- HELPER FUNCTIONS ---
def get_image_path(article_id):
    article_str = str(article_id).zfill(10)
    return os.path.join(IMAGE_DIR, article_str[:3], f"{article_str}.jpg")

def fetch_feed(username):
    """Fetches the smart feed (Global for New, Hybrid for Old)"""
    try:
        # We pass the username so FastAPI knows which algorithm to use
        response = requests.get(f"{API_BASE_URL}/feed/{username}")
        if response.status_code == 200:
            return response.json().get("items", [])
    except:
        return []
    return []

def send_interaction(item_id, action):
    """Sends real telemetry to Kafka + Redis"""
    try:
        requests.post(f"{API_BASE_URL}/interact", json={
            "user_id": st.session_state.kaggle_id,
            "username": st.session_state.username,
            "item_id": str(item_id),
            "action": action
        })
    except:
        pass

# ==========================================
# UI: LOGIN / SIGNUP
# ==========================================
if not st.session_state.logged_in:
    st.title("StyleStream AI")
    st.subheader("Real-Time Hybrid Recommendation Engine")
    
    tab1, tab2 = st.tabs(["Login", "Sign Up"])
    
    with tab1:
        u = st.text_input("Username", key="login_user")
        p = st.text_input("Password", type="password", key="login_pass")
        if st.button("Login"):
            try:
                res = requests.post(f"{API_BASE_URL}/login", json={"username": u, "password": p})
                if res.status_code == 200:
                    data = res.json()
                    st.session_state.logged_in = True
                    st.session_state.username = data["username"]
                    st.session_state.kaggle_id = data["kaggle_id"]
                    st.session_state.user_type = data["user_type"]
                    
                    # --- NEW: Force memory wipe on fresh login ---
                    if 'current_feed' in st.session_state:
                        del st.session_state['current_feed']
                        
                    st.rerun()
                else:
                    st.error("Invalid credentials.")
            except Exception as e:
                st.error(f"Connection Error: {e}")

    with tab2:
        nu = st.text_input("New Username", key="signup_user")
        np_pass = st.text_input("New Password", type="password", key="signup_pass")
        if st.button("Sign Up"):
            try:
                res = requests.post(f"{API_BASE_URL}/signup", json={"username": nu, "password": np_pass})
                if res.status_code == 200:
                    st.success("Account created! Go to Login.")
                else:
                    st.error("Signup failed.")
            except:
                st.error("Backend offline.")

# ==========================================
# UI: STOREFRONT
# ==========================================
else:
    # Header
    c1, c2, c3 = st.columns([6, 2, 1])
    c1.title("StyleStream | Live Feed")
    c2.write(f"👤 **{st.session_state.username}**")
    if c3.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.username = ""
        st.session_state.kaggle_id = ""
        st.session_state.user_type = ""
        st.session_state.cart = []
        if 'current_feed' in st.session_state:
            del st.session_state['current_feed']
            
        st.rerun()
    st.markdown("---")

    # Sidebar Cart
    st.sidebar.title("🛒 Cart")
    if st.session_state.checkout_success:
        st.sidebar.success("✅ Order Placed!")
        st.session_state.checkout_success = False

    if st.session_state.cart:
        for item in st.session_state.cart:
            sc1, sc2 = st.sidebar.columns([1,3])
            try:
                sc1.image(Image.open(get_image_path(item)), use_column_width=True)
            except:
                sc1.write("img")
            sc2.write(f"ID: {item[-6:]}")
        
        if st.sidebar.button("Checkout"):
            for item in st.session_state.cart:
                send_interaction(item, "purchase")
            st.session_state.cart = []
            st.session_state.checkout_success = True
            st.rerun()
    else:
        st.sidebar.write("Empty")

    # MAIN FEED LOGIC
    if 'current_feed' not in st.session_state:
        # Fetch the personalized feed on first load
        st.session_state.current_feed = fetch_feed(st.session_state.username)

    # Dynamic Title based on User Type
    if st.session_state.user_type == "historical":
        st.subheader("✨ Recommended For You (Hybrid LightGBM)")
    else:
        st.subheader("🔥 Global Trending (Real-Time)")

    if not st.session_state.current_feed:
        st.info("Generating feed... (If new, click items to generate trends!)")
    else:
        cols = st.columns(5)
        for idx, article_id in enumerate(st.session_state.current_feed):
            with cols[idx % 5]:
                try:
                    st.image(Image.open(get_image_path(article_id)), use_column_width=True)
                    
                    # Button 1: View Similar
                    if st.button("👁️ Similar", key=f"sim_{idx}"):
                        send_interaction(article_id, "view_similar")
                        # Call visual endpoint
                        res = requests.get(f"{API_BASE_URL}/similar/{article_id}")
                        if res.status_code == 200:
                            st.session_state.current_feed = res.json().get("items", [])
                            st.rerun()

                    # Button 2: Add to Cart
                    if st.button("🛒 Add", key=f"add_{idx}"):
                        st.session_state.cart.append(article_id)
                        send_interaction(article_id, "add_to_cart")
                        st.rerun()
                        
                except FileNotFoundError:
                    st.caption("Image N/A")
    
    # Reset Button
    st.markdown("---")
    if st.button("🏠 Back to Home Feed"):
        st.session_state.current_feed = fetch_feed(st.session_state.username)
        st.rerun()