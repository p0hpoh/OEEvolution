import pandas as pd
import re
from pathlib import Path

folder_path = Path(r"C:\Users\tzeen\Downloads\2024 Data Logs (4)\2024 Logs\PcbVision\PCB\Log\Machine")

# Initialize lists to store data
timestamps = []
log_messages = []
product_names = []
product_ids = []
dates = []
original_lines = []  # Store original lines

seen_products = set()

# Process each log file in the folder
for log_file in sorted(folder_path.glob("*.log")):
    log_filename = log_file.stem  # Extract filename without extension

    # Read the file
    with open(log_file, "r", encoding="latin-1", errors="ignore") as file:
        lines = file.readlines()

    # Process each line
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Extract timestamp and message
        match = re.match(r"(\d{2}:\d{2}:\d{2}):(.*)", line)
        if match:
            timestamp, message = match.groups()

            # ✅ **Only process lines containing "SetFileName"**
            if "SetFileName" not in message:
                continue  # Skip lines that do not contain "SetFileName"

            # Extract full product path from SetFileName File:
            current_product = message.split("SetFileName File:")[-1].strip()

            # ✅ **Extract product name (after the last `\` in the path)**
            current_product_name = current_product.split("\\")[-1]  

            # ✅ **Extract any 8-digit product ID (regardless of prefix)**
            product_id_match = re.search(r"(\d{8,9})", current_product)
            if product_id_match:
                current_product_id = product_id_match.group(1)
                # Mark 9-digit IDs as "error"
                if len(current_product_id) == 9:
                    current_product_id = "error"
            else:
                current_product_id = "99999999"  # Fallback if no ID is found

            # ✅ **Check differences based on Product Name and Product ID**
            product_key = (current_product_name, current_product_id)
            if product_key in seen_products:
                continue  # Skip if already processed

            seen_products.add(product_key)

            # Append data
            product_names.append(current_product_name)
            product_ids.append(current_product_id)
            original_lines.append(line)  # Append original line

# ✅ **Create DataFrame with only the required columns**
df = pd.DataFrame({
    "Original Line": original_lines,  # Original log entry
    "Product Name": product_names,  # Extracted product name
    "Product_ID": product_ids
}).drop_duplicates().reset_index(drop=True)

# ✅ **Sort by Product_ID to group identical IDs together**
df = df.sort_values(by=["Product_ID"])

# Display first few rows
print(df.head(1000))

# Save the output to Excel
output_path = r"C:\Users\tzeen\Downloads\filtered_log_data.xlsx"
df.to_excel(output_path, index=False)

print(f"✅ Excel file with {len(df)} rows saved to: {output_path}")
