import sqlite3
import pandas as pd

print("Connecting to SQLite database (auth.db)...")
# This automatically creates the 'auth.db' file on your hard drive
conn = sqlite3.connect('auth.db')
cursor = conn.cursor()

# 1. Create the Users Table
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password TEXT NOT NULL,
    kaggle_id TEXT NOT NULL,
    user_type TEXT NOT NULL
)
''')

# 2. Load the historical data
print("Loading historical H&M users from CSV...")
df_mapping = pd.read_csv("user_mapping.csv")

# 3. Prepare data for bulk insert
print("Preparing data for migration (this will take a few seconds)...")
historical_users = []
for index, row in df_mapping.iterrows():
    historical_users.append((str(row['user_id_int']), "password123", str(row['customer_id']), "historical"))

# 4. Execute the bulk insert
print(f"Inserting {len(historical_users)} users into SQLite...")
cursor.executemany('''
INSERT OR IGNORE INTO users (username, password, kaggle_id, user_type)
VALUES (?, ?, ?, ?)
''', historical_users)

# Save and close
conn.commit()
conn.close()
print("Successfully initialized auth.db with all historical users!")