import pandas as pd
import numpy as np
from sqlalchemy import create_engine


from sqlalchemy import create_engine, text  # import text

# -----------------------------
# SQL Server Connection (Windows Authentication)
# -----------------------------
server = r'OLUWATOSIN\SQLEXPRESS'
database = 'EntrovaDB'
driver = 'ODBC Driver 17 for SQL Server'

connection_string = f"mssql+pyodbc://@{server}/{database}?driver={driver}&trusted_connection=yes"
engine = create_engine(connection_string)

# Test the connection
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))  # wrap SQL in text()
    print(result.fetchone())  # Should output (1,)


# -----------------------------
# 1. Extract
# -----------------------------
users = pd.read_sql("SELECT * FROM Users", con=engine)
employers = pd.read_sql("SELECT * FROM Employers", con=engine)
demo_sessions = pd.read_sql("SELECT * FROM DemoSessions", con=engine)
engagement = pd.read_sql("SELECT * FROM Engagement", con=engine)

print("Data extracted from SQL Server successfully!")

# -----------------------------
# 2. Transform
# -----------------------------
# Example transformations:
# 2a. Merge user info with engagement
user_engagement = users.merge(engagement, on='user_id', how='left')

# 2b. Merge demo session data
user_demo = demo_sessions.merge(user_engagement, on='user_id', how='left')

# 2c. Merge employer info
full_data = user_demo.merge(employers, left_on='employer_id', right_on='employer_id', how='left')

# 2d. Feature engineering
full_data['days_since_signup'] = (pd.to_datetime('2025-03-15') - pd.to_datetime(full_data['signup_date'])).dt.days
full_data['engagement_score'] = full_data['logins_last_30_days']*0.4 + full_data['messages_sent']*0.6

print("Data transformed successfully!")

# -----------------------------
# 3. Load
# -----------------------------
# Save cleaned/transformed data to folder
full_data.to_csv('../etl/transformed/full_data.csv', index=False)
print("Transformed data loaded to CSV for analytics/ML!")

# Optional: Load back into SQL Server into a new table
full_data.to_sql('FullData', con=engine, if_exists='replace', index=False)
print("Transformed data loaded back into SQL Server table 'FullData'.")