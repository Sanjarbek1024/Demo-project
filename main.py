import os
import io
import json
import requests
import pandas as pd
import urllib
from datetime import datetime
from sqlalchemy import create_engine
import pyodbc

# Names and URLs
SQL_SERVER = "SANJARBEK\\SQLEXPRESS"
CLEANED_DB  = "BankingETL"
FILES_URL   = "https://raw.githubusercontent.com/odilbekmarimov/DemoProject/main/files_final"
MAP_URL     = "https://raw.githubusercontent.com/odilbekmarimov/DemoProject/main/column_table_map.json"

# From Table ID to Human-readable names
TABLE_IDS = {
    "01": "users",
    "02": "cards",
    "03": "transactions",
    "04": "logs",
    "05": "reports",
    "07": "scheduled_payments"
}

CSV_FILES = [f"t{tid}.csv" for tid in TABLE_IDS.keys()]

CLEANED_DIR = "cleaned_data_csv"        # Local cleaned data folder
os.makedirs(CLEANED_DIR, exist_ok=True)

# Engines
def get_engine(db):
    conn_str = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={db};"
        f"Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

cleaned_engine = get_engine(CLEANED_DB)

# Connection with SQL server
odbc_conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={CLEANED_DB};Trusted_Connection=yes;"
)
cursor = odbc_conn.cursor()
cursor.execute("""
IF OBJECT_ID('dbo.retrieveinfo','U') IS NULL
CREATE TABLE dbo.retrieveinfo (
    retrieve_id INT IDENTITY(1,1) PRIMARY KEY,
    source_file NVARCHAR(255),
    retrieved_at DATETIME,
    total_rows INT,
    processed_rows INT,
    errors INT,
    notes NVARCHAR(500)
)
""")
odbc_conn.commit()

# Loading column map
resp = requests.get(MAP_URL); resp.raise_for_status()
column_map = resp.json()

# Log file
LOG_FILE = "retrieveinfo_log.txt"
with open(LOG_FILE, "w", encoding="utf-8") as log_f:
    log_f.write("source_file,retrieved_at,total_rows,processed_rows,errors,notes\n")

for csv_name in CSV_FILES:
    tid = csv_name[1:3]
    table = TABLE_IDS[tid]
    url = f"{FILES_URL}/{csv_name}"
    print(f"\nDownloading {csv_name} ({table}) ...")

    #Downloading table files
    try:
        r = requests.get(url); r.raise_for_status()
        df_raw = pd.read_csv(io.StringIO(r.text))
    except Exception as e:
        print(f"Download error: {e}")
        cursor.execute(
            "INSERT INTO retrieveinfo (source_file, retrieved_at, total_rows, processed_rows, errors, notes) VALUES (?,?,?,?,?,?)",
            f"{table}.csv", datetime.now(), 0, 0, 1, str(e)
        )
        odbc_conn.commit()
        with open("retrieveinfo_log.txt", "a", encoding="utf-8") as log_f:
            log_f.write(f"{table}.csv,{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},0,0,1,error download\n")
        continue

    total_rows = len(df_raw)

    cols = column_map.get(tid, {}).get("columns", {})
    rename_dict = {f"{tid}-{k}":v for k,v in cols.items()}
    df = df_raw.rename(columns=rename_dict).copy()
    df.columns = df.columns.str.strip()

    if "id" not in df.columns:
        df.insert(0, "id", range(1, len(df)+1))

    if "is_vip" in df:     
        df["is_vip"] = df["is_vip"].astype(bool)
    if "is_blocked" in df:  
        df["is_blocked"] = df["is_blocked"].astype(bool)
    for num in ["amount", "total_balance", "balance", "limit_amount",
                 "total_transactions", "flagged_transactions", "total_amount"]:
        if num in df:
            df[num] = pd.to_numeric(df[num], errors="coerce").fillna(0)
    for dt in df.columns:
        if dt.endswith("_at") or "date" in dt:
            df[dt] = pd.to_datetime(df[dt], errors="coerce")

    clean_csv = f"{table}.csv"
    df.to_csv(f"{CLEANED_DIR}/{clean_csv}", index=False)
    df.to_sql(table, cleaned_engine, if_exists='replace', index=False)
    print(f"CLEANED -> {CLEANED_DB}.{table}")

    cursor.execute(
        "INSERT INTO retrieveinfo (source_file, retrieved_at, total_rows, processed_rows, errors, notes) VALUES (?,?,?,?,?,?)",
        f"{table}.csv", datetime.now(), total_rows, len(df), 0, "loaded"
    )
    odbc_conn.commit()
    with open("retrieveinfo_log.txt", "a", encoding="utf-8") as log_f:
        log_f.write(f"{table}.csv,{datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{total_rows},{len(df)},0,loaded\n")

print("\nGenerating derived tables...\n")

df_users        = pd.read_csv(os.path.join(CLEANED_DIR, "users.csv"))
df_cards        = pd.read_csv(os.path.join(CLEANED_DIR, "cards.csv"))
df_transactions = pd.read_csv(os.path.join(CLEANED_DIR, "transactions.csv"))

# Fraud Detection
fd = df_transactions.merge(
    df_cards[["id", "limit_amount", "user_id"]],
    left_on="from_card_id", right_on="id",
    suffixes=("_txn", "_card")
)
fd = fd[fd["amount"] > fd["limit_amount"]].copy()
fd["reason"] = "Amount exceeds card limit"
fd["status"] = "flagged"

# Safely get columns
fraud_out = pd.DataFrame()
fraud_out["transaction_id"] = fd["id_txn"]
fraud_out["from_card_id"] = fd["from_card_id"]
fraud_out["user_id"] = fd["user_id"]
fraud_out["reason"] = fd["reason"]
fraud_out["status"] = fd["status"]
fraud_out["created_at"] = pd.to_datetime(fd["created_at"], errors="coerce")

fraud_out.to_csv(f"{CLEANED_DIR}/fraud_detection.csv", index=False)
fraud_out.to_sql("fraud_detection", cleaned_engine, if_exists='replace', index=False)
print("fraud_detection created")

# VIP Users
df_users["total_balance"] = pd.to_numeric(df_users["total_balance"], errors="coerce")
vip = df_users[df_users["total_balance"] > 5e8].copy()
vip["assigned_at"] = datetime.now()
vip["reason"] = "High balance"

vip_out = vip[["id", "assigned_at", "reason"]].copy()
vip_out.columns = ["user_id", "assigned_at", "reason"]
vip_out.to_csv(f"{CLEANED_DIR}/vip_users.csv", index=False)
vip_out.to_sql("vip_users", cleaned_engine, if_exists='replace', index=False)
print("vip_users created")

# Blocked Users
df_cards["balance"] = pd.to_numeric(df_cards["balance"], errors="coerce")
blk = df_cards[df_cards["balance"] < 0].copy()
blk["reason"] = "Negative balance"
blk["blocked_at"] = datetime.now()

blk_out = blk[["id", "reason", "blocked_at"]].copy()
blk_out.columns = ["card_id", "reason", "blocked_at"]
blk_out.to_csv(f"{CLEANED_DIR}/blocked_users.csv", index=False)
blk_out.to_sql("blocked_users", cleaned_engine, if_exists='replace', index=False)
print("blocked_users created")

cursor.close()
odbc_conn.close()
print("\nPython is finished huurrray! :D")

