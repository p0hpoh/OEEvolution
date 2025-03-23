import pandas as pd
import re
import pyodbc
from pathlib import Path

# 🚀 1️⃣ Database Connection Setup
server = '(local)\\SQLEXPRESS'  # Change if needed
database = 'OEEvolution'  # Change to your actual database name
table_name = 'LogData'  # Table name in SQL Server

# Establish connection to SQL Server Express
conn = pyodbc.connect(
    f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes'
)
cursor = conn.cursor()

# 🚀 2️⃣ Create Table if Not Exists
create_table_query = f"""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
BEGIN
    CREATE TABLE {table_name} (
        filename NVARCHAR(255),
        timestamp TIME,
        action NVARCHAR(MAX)
    )
END
"""
cursor.execute(create_table_query)
conn.commit()

# 🚀 3️⃣ Extracting Data from Log Files
folder_path = r"C:\Users\tzeen\Downloads\Sample dates DBA AAAAHHHH\2023.11.15.log"  # Adjust to your Windows path

# Store extracted data
all_data = []

# Add action column if not exists
add_action_column_query = """
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
               WHERE TABLE_NAME = 'LogData' AND COLUMN_NAME = 'action')
BEGIN
    ALTER TABLE LogData
    ADD action NVARCHAR(MAX);
END
"""
cursor.execute(add_action_column_query)
conn.commit()

# Function to extract the action from each line
def extract_action(line):
    match = re.match(r"(\d{2}:\d{2}:\d{2}):(.*)", line)
    if match:
        timestamp, action = match.groups()
        return timestamp, action.strip()
    return None, None

# Loop through all log files in the specified folder
for log_file in Path(folder_path).glob("*.log"):
    with open(log_file, "r", encoding="latin-1", errors="ignore") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue

            # Extract timestamp and action from each log line
            timestamp, action = extract_action(line)
            if timestamp and action:
                all_data.append({
                    "filename": log_file.name,
                    "timestamp": timestamp,
                    "action": action
                })

# Convert to DataFrame for further inspection
df = pd.DataFrame(all_data)

# Debugging: Ensure Data is Extracted Correctly
if df.empty:
    print("⚠️ No data extracted! Check log files or folder path.")
else:
    print(f"✅ Extracted {len(df)} rows. Sample data:")
    print(df.head())

# Insert Data into SQL Server (if Data Exists)
if not df.empty:
    insert_query = f"INSERT INTO {table_name} (filename, timestamp, action) VALUES (?, ?, ?)"

    for index, row in df.iterrows():
        try:
            cursor.execute(insert_query, row["filename"], row["timestamp"], row["action"])
            print(f"Inserted row {index + 1}: {row['filename']} | {row['timestamp']} | {row['action']}")
        except Exception as e:
            print(f"❌ Error inserting row {index + 1}: {e}")

    # 🚀 6️⃣ Commit changes & Close Connection
    conn.commit()
    print("✅ Data successfully loaded into SQL Server Express!")

# 🚀 4️⃣ Query Data from SQL Server
query = f"SELECT TOP 10 * FROM {table_name};"  # Adjust this query as needed to retrieve your data
df_sql = pd.read_sql(query, conn)

# 🚀 5️⃣ Display Data (you can manipulate the data as needed)
print("✅ Data fetched from SQL Server:")
print(df_sql)

# Close the connection when you're done
cursor.close()
conn.close()
