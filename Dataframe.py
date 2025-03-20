import pandas as pd
import re
from pathlib import Path

# ✅ Corrected: Use raw string OR forward slashes
folder_path = Path(r"C:\Users\Jerald\Documents\Uni Docs\Term 4\Data Business and Analytics\Project\Datasets\2024 Logs\PcbVision\PCB\Log\Machine")

# Initialize lists to store data
timestamps = []
log_messages = []
statuses = []
products = []
product_ids = []
dates = []

# Process each log file in the folder
for log_file in folder_path.glob("*.log"):
    log_filename = log_file.stem  # Extract filename without extension

    # Read the file
    with open(log_file, "r", encoding="latin-1", errors="ignore") as file:
        lines = file.readlines()

    # Initialize tracking variables
    current_status = "Idle"  
    current_product = ""  
    current_product_id = "99999999"  # Default Product ID

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

                # ✅ Fix: Improved regex to handle both `/` and `\` as directory separators
                product_id_match = re.search(r"[/\\](\d{8})_", current_product)
                if product_id_match:
                    current_product_id = product_id_match.group(1)
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

import pandas as pd


# To create CSV File on Excel to see the 1M rows from the middle of the dataframe
'''
# Define the output file path
output_file = r"C:\Users\Jerald\Documents\GitHub\OEEvolution\middle_1M_rows.xlsx"

# Get the total number of rows
total_rows = len(df)

# Define the range to extract 1 million rows from the middle
start_index = max(0, total_rows // 2 - 500000)  # Ensure we don't go below index 0
end_index = min(total_rows, start_index + 1000000)  # Ensure we don't exceed max rows

# Extract middle 1 million rows
df_middle = df.iloc[start_index:end_index]

# Save to Excel
df_middle.to_excel(output_file, index=False, engine="openpyxl")

# Print confirmation
print(f"Middle 1 million rows saved to: {output_file}")'
'''

