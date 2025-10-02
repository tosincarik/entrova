import pandas as pd
import os
import pyodbc
from sqlalchemy import create_engine
import logging
from datetime import datetime

# -------------------------------
# 1. Setup Logging
# -------------------------------
# Repo-based logs folder
base_dir = os.path.dirname(os.path.abspath(__file__))  # etl/ folder
repo_root = os.path.abspath(os.path.join(base_dir, ".."))  # go up one level

log_dir = os.path.join(repo_root, "logs")
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(
    log_dir, f"ETL_log_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.log"
)

#log_file = f"C:\\ETL\\logs\\ETL_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file,
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

logging.info("ETL process started")

try:
    # -------------------------------
    # 2. Connect to SQL Server
    # -------------------------------
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=OLUWATOSIN\\SQLEXPRESS;'
        'DATABASE=EntrovaDB;'
        'Trusted_Connection=yes;'
    )
    engine = create_engine("mssql+pyodbc://OLUWATOSIN\\SQLEXPRESS/EntrovaDB?driver=ODBC+Driver+17+for+SQL+Server")

    logging.info("Connected to SQL Server successfully")

    # -------------------------------
    # 3. Extract Data
    # -------------------------------
    users_df = pd.read_sql("SELECT * FROM dbo.users", conn)
    demo_df = pd.read_sql("SELECT * FROM dbo.DemoSessions", conn)
    engagement_df = pd.read_sql("SELECT * FROM dbo.engagement", conn)
    
    logging.info(f"Extracted {len(users_df)} users, {len(demo_df)} demo sessions, {len(engagement_df)} engagement records")

    # -------------------------------
    # 4. Transform Users
    # -------------------------------
    users_df['signup_date_parsed'] = pd.to_datetime(users_df['signup_date'])
    users_df['country'] = users_df['country'].fillna('Unknown')

    # Cumulative users
    user_cum = users_df.groupby('signup_date_parsed')['user_id'].nunique().cumsum().reset_index()
    user_cum.rename(columns={'user_id': 'daily_users'}, inplace=True)

    logging.info("Transformed Users data")

    # -------------------------------
    # 5. Transform Demo Sessions
    # -------------------------------
    demo_df['demo_date'] = pd.to_datetime(demo_df['demo_date'])
    demo_df['attended_demo'] = demo_df['attended_demo'].fillna(0).astype(int)
    demo_attendance = demo_df.groupby('demo_date')['attended_demo'].sum().reset_index()
    demo_attendance.rename(columns={'attended_demo': 'total_attended_demo'}, inplace=True)

    logging.info("Transformed Demo Sessions data")

    # -------------------------------
    # 6. Transform Engagement
    # -------------------------------
    engagement_df['logins_last_30_days'] = engagement_df['logins_last_30_days'].fillna(0).astype(int)
    engagement_df['messages_sent'] = engagement_df['messages_sent'].fillna(0).astype(int)
    engagement_df['profile_completed'] = engagement_df['profile_completed'].fillna(0).astype(int)

    # Derived columns
    engagement_df['active_user'] = engagement_df['logins_last_30_days'].apply(lambda x: 1 if x>0 else 0)
    engagement_df['sent_messages'] = engagement_df['messages_sent'].apply(lambda x: 1 if x>0 else 0)
    engagement_df['profile_done'] = engagement_df['profile_completed'].apply(lambda x: 1 if x>0 else 0)

    # Bring signup_date from Users
    engagement_metrics = engagement_df.merge(users_df[['user_id','signup_date_parsed']], on='user_id', how='left')

    # Aggregate by signup_date
    engagement_summary = engagement_metrics.groupby('signup_date_parsed')\
                        .agg({'active_user':'sum', 'sent_messages':'sum', 'profile_done':'sum'})\
                        .reset_index()
    engagement_summary.rename(columns={'active_user':'active_users_sum',
                                       'sent_messages':'messages_sent_sum',
                                       'profile_done':'profile_completed_sum'}, inplace=True)

    logging.info("Transformed Engagement data")

    # -------------------------------
    # 7. Load to SQL Server
    # -------------------------------
    user_cum.to_sql('user_cumulative_signups', engine, schema='analytics', if_exists='replace', index=False)
    demo_attendance.to_sql('demo_attendance_summary', engine, schema='analytics', if_exists='replace', index=False)
    engagement_summary.to_sql('engagement_summary', engine, schema='analytics', if_exists='replace', index=False)

    logging.info("Loaded transformed data into SQL Server successfully")

except Exception as e:
    logging.error(f"ETL failed: {str(e)}")
    raise

logging.info("ETL process completed successfully")
