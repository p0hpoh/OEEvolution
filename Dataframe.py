import pandas as pd
import re
from pathlib import Path

# ✅ Corrected: Use raw string OR forward slashes
folder_path = Path(r"C:\Users\shery\Downloads\SUTD\DBA\2024 Logs\PcbVision\PCB\Log\Machine")

# Initialize lists to store data
timestamps = []
log_messages = []
statuses = []
products = []
product_ids = []
dates = []

# ✅ Initialize global tracking variables to persist across log files
last_product = ""  
last_product_id = "99999999"  

# Process each log file in the folder
for log_file in sorted(folder_path.glob("*.log")):  # ✅ Ensure logs are processed in order
    log_filename = log_file.stem  # Extract filename without extension

    # Read the file
    with open(log_file, "r", encoding="latin-1", errors="ignore") as file:
        lines = file.readlines()

    # Initialize tracking variables
    current_status = "Idle"  
    current_product = last_product  # ✅ Start with the last known product
    current_product_id = last_product_id  # ✅ Start with the last known Product ID

    # Process each line
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Extract timestamp and message
        match = re.match(r"(\d{2}:\d{2}:\d{2}):(.*)", line)
        if match:
            timestamp, message = match.groups()

            # ✅ Fix: Extract product name and Product ID
            if "SetFileName File:" in message:
                current_product = message.split("SetFileName File:")[-1].strip()
                last_product = current_product  # ✅ Update global product

                # ✅ Fix: Improved regex to handle both `/` and `\` as directory separators
                product_id_match = re.search(r"[/\\](\d{8})_", current_product)
                if product_id_match:
                    current_product_id = product_id_match.group(1)
                    last_product_id = current_product_id  # ✅ Update global product ID
                else:
                    current_product_id = "99999999"  

            # Status transitions
            if "(0)--Start Mark!--" in message:
                current_status = "Productive"
            elif "Successfully Cutting" in message:
                current_status = "Idle"
            elif "Stop PLC!" in message or "The Software Stop Button is Pressed" in message:
                current_status = "Idle"
            elif "----Start Procession: Manufacture----" in message:
                current_status = "Standby"
            elif "Err:" in message:
                current_status = "Downtime"

            # Append data
            timestamps.append(timestamp)
            log_messages.append(message)
            statuses.append(current_status)
            products.append(current_product)
            product_ids.append(current_product_id)
            dates.append(log_filename)

# ✅ Create DataFrame
df = pd.DataFrame({
    "Date": dates,
    "Timestamp": timestamps,
    "Log Message": log_messages,
    "Product": products,
    "Product_ID": product_ids,  # New Product_ID column
    "Status": statuses
})

# ✅ Prevent pandas from truncating output
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

# ✅ Display first 1000 rows
print(df.head(1000))

# ✅ CSV row limit (excluding header)
csv_limit = 1_048_575

# ✅ Calculate starting index (75% of the DataFrame)
start_index = int(len(df) * 0.75)

# ✅ Slice safely without exceeding CSV row limit
df_slice = df.iloc[start_index:start_index + min(csv_limit, len(df) - start_index)]

# ✅ Output file path
output_path = r"C:\Users\shery\Downloads\SUTD\DBA\log_data_slice.csv"

# ✅ Write to Excel (openpyxl is default for .xlsx)
df_slice.to_csv(output_path, index=False)

print(f"✅ CSV file with {len(df_slice)} rows saved to: {output_path}")
