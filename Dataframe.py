import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# ✅ Define the folder path
folder_path = Path(r"C:\Users\Jerald\Documents\Uni Docs\Term 4\Data Business and Analytics\Project\Datasets\2024 Logs\PcbVision\PCB\Log\Machine")

# ✅ Initialize lists to store data
timestamps = []
log_messages = []
statuses = []
products = []
product_ids = []
time_diffs = []
dates = []

# ✅ Initialize global tracking variables
last_product = ""  
last_product_id = "99999999"  
current_base_status = "None"
previous_timestamp = None
previous_label = "None"
previous_non_downtime_status = "None"  # ✅ Keeps track of last status before downtime

def base_status_of(label: str) -> str:
    """ Convert "Start Status" -> "Status" and "End Status" -> "Status" """
    if label.startswith("Start "):
        return label.replace("Start ", "")
    if label.startswith("End "):
        return label.replace("End ", "")
    return label

def compute_time_diff(prev_ts, current_ts, prev_label, current_label):
    """ Compute time difference only if the base statuses match """
    if prev_ts is None:
        return None
    prev_base = base_status_of(prev_label)
    this_base = base_status_of(current_label)
    if prev_base == this_base and prev_base not in ["None", "Downtime"]:
        return (current_ts - prev_ts).total_seconds()
    return None

# ✅ Process each log file in sorted order
for log_file in sorted(folder_path.glob("*.log")):
    log_filename = log_file.stem  # Extract filename without extension

    with open(log_file, "r", encoding="latin-1", errors="ignore") as file:
        lines = file.readlines()

    if not lines:
        continue  # Skip empty files

    # ✅ Reset for each file but retain product information across days
    previous_timestamp = None
    previous_label = "None"
    current_product = last_product
    current_product_id = last_product_id

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # ✅ Extract timestamp and message
        match = re.match(r"(\d{2}:\d{2}:\d{2}):(.*)", line)
        if match:
            timestamp_str, message = match.groups()
            current_dt = datetime.strptime(timestamp_str, "%H:%M:%S")

            # ✅ Extract product name and Product ID
            if "SetFileName File:" in message:
                current_product = message.split("SetFileName File:")[-1].strip()
                last_product = current_product  # ✅ Update global product

                # ✅ Handle both _ and  -  separators
                product_id_match = re.search(r"[/\\](\d{8})[_-]", current_product)
                if product_id_match:
                    current_product_id = product_id_match.group(1)
                    last_product_id = current_product_id  # ✅ Update global product ID
                else:
                    current_product_id = "99999999"

            line_lower = message.lower()
            new_label = current_base_status

            # ✅ Status Transitions
            if "start mark" in line_lower or "(0)--start mark!--" in line_lower:
                if current_base_status == "Standby" and statuses:
                    prev_label_val = statuses[-1]
                    if base_status_of(prev_label_val) == "Standby":
                        statuses[-1] = "End Standby"
                    current_base_status = "None"

                new_label = "Start Productive"
                current_base_status = "Productive"

            elif "successfully cutting" in line_lower and current_base_status == "Productive":
                new_label = "End Productive"
                current_base_status = "None"

            elif ("stop plc!" in line_lower or "the software stop button is pressed" in line_lower) and current_base_status == "None":
                new_label = "Start Idle"
                current_base_status = "Idle"

            elif "alarm" in line_lower and "reset" in line_lower and current_base_status == "Idle":
                new_label = "End Idle"
                current_base_status = "None"

            elif "start procession" in line_lower and "manufacture" in line_lower and current_base_status == "None":
                new_label = "Start Standby"
                current_base_status = "Standby"

            # ✅ Downtime Handling
            elif "err:" in line_lower:
                new_label = "Downtime"
                previous_non_downtime_status = current_base_status  # ✅ Store last non-downtime status
                current_base_status = "None"  # ✅ Set to None temporarily

            elif current_base_status == "None" and previous_non_downtime_status != "None":
                # ✅ When Downtime ends, return to previous status
                new_label = previous_non_downtime_status
                current_base_status = previous_non_downtime_status
                previous_non_downtime_status = "None"  # ✅ Reset stored status

            else:
                if current_base_status in ["Productive", "Idle", "Standby"]:
                    new_label = current_base_status
                elif current_base_status == "Downtime":
                    new_label = "Downtime"
                elif current_base_status == "None":
                    new_label = "None"

            # ✅ Compute time difference
            time_diff = compute_time_diff(previous_timestamp, current_dt, previous_label, new_label)

            # ✅ Append data
            timestamps.append(timestamp_str)
            log_messages.append(message)
            statuses.append(new_label)
            products.append(current_product)
            product_ids.append(current_product_id)
            dates.append(log_filename)
            time_diffs.append(time_diff)

            # ✅ Update tracking variables
            previous_timestamp = current_dt
            previous_label = new_label

# ✅ Create DataFrame
df = pd.DataFrame({
    "Date": dates,
    "Timestamp": timestamps,
    "Log Message": log_messages,
    "Product": products,
    "Product_ID": product_ids,
    "Status": statuses,
    "Time Difference (Seconds)": time_diffs
})

# ✅ Prevent pandas from truncating output
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

# ✅ Display first 1000 rows
print(df.head(1000))

# ✅ Excel row limit (excluding header)
excel_limit = 1_048_575

# ✅ Calculate starting index (75% of the DataFrame)
start_index = int(len(df) * 0.75)

# ✅ Slice safely without exceeding Excel row limit
df_slice = df.iloc[start_index:start_index + min(excel_limit, len(df) - start_index)]

# ✅ Output file path
output_path = r"C:\Users\Jerald\Downloads\log_data_slice.xlsx"

# ✅ Write to Excel (openpyxl is default for .xlsx)
df_slice.to_excel(output_path, index=False)

print(f"✅ Excel file with {len(df_slice)} rows saved to: {output_path}")
