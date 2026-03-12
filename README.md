# 🛍️ StyleStream AI: Real-Time Hybrid Recommendation Engine

An end-to-end, event-driven recommendation system for e-commerce. This project blends historical machine learning personalization (ALS + LightGBM) with real-time viral stream processing (Kafka + PySpark) to solve the "Cold Start" problem and balance Exploitation vs. Exploration in user feeds.

##  System Architecture

The pipeline is built on a modern, decoupled microservices architecture designed to process user telemetry instantly and serve sub-second recommendations.

1. **Frontend (Streamlit):** Simulates the e-commerce UI. Captures user interactions (views, cart adds, purchases).
2. **Backend Engine (FastAPI):** Acts as the "Brain." Routes telemetry to the data pipeline and serves the hybrid recommendation feeds.
3. **Event Streaming (Apache Kafka):** Ingests live user clickstream data in real-time.
4. **Stream Processing (PySpark):** Consumes Kafka events, applies a weighted scoring algorithm (`View=1`, `Cart=3`, `Purchase=10`), and calculates live global trends.
5. **In-Memory Cache (Redis):** Stores the live trending leaderboard for instant retrieval by the backend.
6. **Machine Learning Pipeline:**
   - **Candidate Generation:** FAISS (Facebook AI Similarity Search) for visual product similarity.
   - **Latent Feature Extraction:** ALS (Alternating Least Squares) Matrix Factorization for user/item historical profiles.
   - **Ranking:** LightGBM pairwise ranker predicts the exact probability of a user purchasing a candidate item.

##  Core Features

* **The 2/3 Slotting Blender:** A production-grade feed generation strategy. Historical users receive a blended feed of **40% hyper-personalized items** (scored via LightGBM against their ALS profile) and **60% viral trending items** (from the live PySpark stream). 
* **Real-Time Weighted Leaderboard:** PySpark Streaming dynamically recalculates global trends based on actual user intent, heavily weighting purchases over casual views.
* **Cold-Start Handling:** New users seamlessly default to the live global trending feed, ensuring a 0-second cold start delay.
* **"View Similar" Visual Search:** Uses deep learning image embeddings indexed in FAISS to instantly retrieve visually similar clothing items.

##  Tech Stack

* **Languages:** Python 3.9+
* **Data Engineering:** Apache Kafka, PySpark Structured Streaming, Redis, SQLite
* **Machine Learning:** LightGBM, FAISS, Implicit (ALS), NumPy, Pandas
* **Web Serving:** FastAPI, Uvicorn, Streamlit, Docker

##  Local Installation & Setup

### 1. Prerequisites
* Docker Desktop installed and running.
* Python 3.9+ installed.
* Java 8+ and Hadoop/Spark binaries configured for your OS.

### 2. Environment Setup
Clone the repository and install the dependencies:
```bash
git clone [https://github.com/yourusername/stylestream-ai.git](https://github.com/yourusername/stylestream-ai.git)
cd stylestream-ai
pip install -r requirements.txt