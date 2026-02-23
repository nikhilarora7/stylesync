import streamlit as st
import requests
import os
from PIL import Image

API_BASE_URL = "http://127.0.0.1:8000"
IMAGE_DIR = r"D:\entahhhtainmentttttt\h-and-m-personalized-fashion-recommendations\images" # Ensure this points to your H&M images

st.set_page_config(page_title="StyleStream AI", layout="wide")

# --- HELPER FUNCTIONS ---
def get_image_path(article_id):
    article_str = str(article_id).zfill(10)
    return os.path.join(IMAGE_DIR, article_str[:3], f"{article_str}.jpg")

def fetch_trending():
    try:
        response = requests.get(f"{API_BASE_URL}/trending")
        if response.status_code == 200:
            return response.json().get("items", [])
    except:
        pass
    return []

# --- SESSION STATE INITIALIZATION ---
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.session_state.kaggle_id = ""
    st.session_state.user_type = ""
if 'cart' not in st.session_state:
    st.session_state.cart = []
if 'checkout_success' not in st.session_state:
    st.session_state.checkout_success = False

# ==========================================
# UI: LOGIN / SIGNUP PAGE (If not logged in)
# ==========================================
if not st.session_state.logged_in:
    st.title("Welcome to StyleStream")
    st.markdown("Please log in to access your personalized feed.")
    
    tab1, tab2 = st.tabs(["Login", "Sign Up"])
    
    with tab1:
        st.subheader("Login")
        st.info("💡 **Hint for testing:** Try logging in with username `1`, `2`, or `3` and password `password123` to test historical H&M users!")
        
        login_user = st.text_input("Username (Type an integer for H&M user)", key="login_user")
        login_pass = st.text_input("Password", type="password", key="login_pass")
        
        if st.button("Login"):
            res = requests.post(f"{API_BASE_URL}/login", json={"username": login_user, "password": login_pass})
            if res.status_code == 200:
                data = res.json()
                st.session_state.logged_in = True
                st.session_state.username = data["username"]
                st.session_state.kaggle_id = data["kaggle_id"]
                st.session_state.user_type = data["user_type"]
                st.rerun()
            else:
                st.error("Invalid credentials.")

    with tab2:
        st.subheader("Create a New Account")
        st.write("Simulate a brand new 'Cold Start' user.")
        signup_user = st.text_input("New Username", key="signup_user")
        signup_pass = st.text_input("New Password", type="password", key="signup_pass")
        
        if st.button("Sign Up"):
            res = requests.post(f"{API_BASE_URL}/signup", json={"username": signup_user, "password": signup_pass})
            if res.status_code == 200:
                st.success("Account created! You can now log in.")
            else:
                st.error(res.json().get("detail", "Signup failed."))

# ==========================================
# UI: MAIN STOREFRONT (If logged in)
# ==========================================
else:
    # --- TOP NAV BAR ---
    col1, col2, col3 = st.columns([6, 2, 1])
    with col1:
        st.title("StyleStream | Live Feed")
    with col2:
        st.write("") 
        st.write(f"👤 **{st.session_state.username}** ({st.session_state.user_type.capitalize()})")
    with col3:
        st.write("") 
        if st.button("Logout 🚪"):
            st.session_state.logged_in = False
            st.session_state.username = ""
            st.session_state.kaggle_id = ""
            st.session_state.cart = []
            if 'current_feed' in st.session_state:
                del st.session_state['current_feed']
            st.rerun()

    st.markdown("---")

    # --- SIDEBAR: SHOPPING CART ---
    st.sidebar.title("🛒 Your Cart")
    
    # 1. The Green Success Popup Logic
    if st.session_state.checkout_success:
        st.sidebar.success("✅ Order Placed! Profile Updated.")
        st.session_state.checkout_success = False # Reset so it disappears on next click
        
    if len(st.session_state.cart) == 0:
        st.sidebar.write("Your cart is empty.")
    else:
        st.sidebar.write(f"Items in cart: {len(st.session_state.cart)}")
        
        # 2. Display Small Cart Thumbnails
        for item in st.session_state.cart:
            cart_col1, cart_col2 = st.sidebar.columns([1, 3])
            img_path = get_image_path(item)
            try:
                cart_col1.image(Image.open(img_path), use_column_width=True)
            except FileNotFoundError:
                cart_col1.write("N/A")
            cart_col2.write(f"ID: {str(item)[-6:]}") # Show just the last 6 digits for space
            
        # 3. The Checkout Logic
        if st.sidebar.button("Checkout & Buy Now"):
            for item in st.session_state.cart:
                 requests.post(f"{API_BASE_URL}/interact", json={
                    "user_id": st.session_state.kaggle_id,
                    "item_id": str(item),
                    "action": "purchase"
                 })
            st.session_state.cart = [] # Empty cart
            st.session_state.checkout_success = True # Trigger the popup
            st.rerun()

    # --- MAIN FEED ---
    st.subheader("🔥 Global Trending Right Now")
    
    if 'current_feed' not in st.session_state:
        st.session_state.current_feed = fetch_trending()
        
    if not st.session_state.current_feed:
        st.info("Waiting for data... Keep the Kafka Producer and Spark Consumer running!")
    else:
        cols = st.columns(5)
        for idx, article_id in enumerate(st.session_state.current_feed):
            with cols[idx % 5]:
                img_path = get_image_path(article_id)
                try:
                    st.image(Image.open(img_path), use_column_width=True)
                    
                    # 4. TWO BUTTONS PER ITEM
                    if st.button("👁️ View Similar", key=f"sim_{article_id}_{idx}"):
                        # Log the interaction
                        requests.post(f"{API_BASE_URL}/interact", json={
                            "user_id": st.session_state.kaggle_id,
                            "item_id": str(article_id),
                            "action": "view_similar"
                        })
                        # Fetch hybrid recommendations, passing the username!
                        res = requests.get(f"{API_BASE_URL}/similar/{article_id}?username={st.session_state.username}")
                        if res.status_code == 200:
                            st.session_state.current_feed = res.json().get("items", [])
                            st.rerun()

                    if st.button("🛒 Add to Cart", key=f"cart_{article_id}_{idx}"):
                        st.session_state.cart.append(article_id)
                        requests.post(f"{API_BASE_URL}/interact", json={
                            "user_id": st.session_state.kaggle_id,
                            "item_id": str(article_id),
                            "action": "add_to_cart"
                        })
                        st.rerun() 
                        
                except FileNotFoundError:
                    st.info(f"Image Missing\nID: {article_id}")