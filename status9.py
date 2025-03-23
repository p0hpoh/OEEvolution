import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# ========== CONFIG ==========
# folder_path = Path("/Users/sabaiyi/Desktop/SUTD/term4/DBA/project/2024 Logs/PcbVision/PCB/Log/Machine copy")
folder_path = Path(r"C:\Users\shery\Downloads\SUTD\DBA\2024 Logs\PcbVision\PCB\Log\Machine")

# ========== DataFrame Lists ==========
timestamps = []
log_messages = []
statuses = []
products = []
product_ids = []
time_diffs = []
dates = []

# ========== Global Variables ==========
last_product = ""
last_product_id = "99999999"
global_base_status = "Standby"

# ========== Utility Functions ==========
def base_status_of(label: str) -> str:
    if label.startswith("Start "):
        return label.replace("Start ", "")
    if label.startswith("End "):
        return label.replace("End ", "")
    return label

def compute_time_diff(prev_ts, current_ts, prev_label, current_label):
    if prev_ts is None:
        return None
    prev_base = base_status_of(prev_label)
    this_base = base_status_of(current_label)
    if prev_base == this_base and prev_base != "Downtime":
        return (current_ts - prev_ts).total_seconds()
    return None

def end_previous_status():
    if statuses:
        last_label = statuses[-1]
        last_base = base_status_of(last_label)
        if last_base in ["Productive", "Idle", "Standby"]:
            statuses[-1] = f"End {last_base}"

FALLBACK_STATUS = "Standby"

# ========== Process Each File in Sorted Order ==========
for log_file in sorted(folder_path.glob("*.log")):
    file_name = log_file.stem

    with open(log_file, "r", encoding="latin-1", errors="ignore") as f:
        lines = f.readlines()
    if not lines:
        continue

    current_base_status = global_base_status
    previous_timestamp = None
    previous_label = current_base_status

    current_product = last_product
    current_product_id = last_product_id

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        match = re.match(r"(\d{2}:\d{2}:\d{2}):(.*)", line)
        if not match:
            continue

        timestamp_str, message = match.groups()
        current_dt = datetime.strptime(timestamp_str, "%H:%M:%S")

        if "SetFileName File:" in message:
            current_product = message.split("SetFileName File:")[-1].strip()
            last_product = current_product
            pid_match = re.search(r"[/\\](\d{8})[_-]", current_product)
            if pid_match:
                current_product_id = pid_match.group(1)
                last_product_id = current_product_id
            else:
                current_product_id = "99999999"

        line_lower = message.lower()
        backup_status = current_base_status
        new_label = backup_status

        if "(0)--start mark!--" in line_lower:
            if backup_status != "Downtime":
                end_previous_status()
            new_label = "Start Productive"
            current_base_status = "Productive"

        elif "successfully cutting" in line_lower and backup_status == "Productive":
            new_label = "End Productive"
            current_base_status = FALLBACK_STATUS

        elif "(0)stop plc!" in line_lower or "the software stop button is pressed" in line_lower:
            if backup_status != "Downtime":
                end_previous_status()
            new_label = "Start Idle"
            current_base_status = "Idle"

        elif "alarm" in line_lower and "reset" in line_lower and backup_status == "Idle":
            new_label = "End Idle"
            current_base_status = FALLBACK_STATUS

        else:
            temp = line_lower.strip("- ").strip()
            if "start procession" in temp and "manufacture" in temp:
                if backup_status != "Downtime":
                    end_previous_status()
                new_label = "Start Standby"
                current_base_status = "Standby"
            elif "err:" in line_lower:
                new_label = "Downtime"
                current_base_status = backup_status
            else:
                new_label = backup_status

        time_diff = None
        if statuses:
            prev_base = base_status_of(statuses[-1])
            this_base = base_status_of(new_label)
            if prev_base == this_base and this_base not in [FALLBACK_STATUS, "Downtime"]:
                prev_ts_str = timestamps[-1]
                prev_dt = datetime.strptime(prev_ts_str, "%H:%M:%S")
                time_diff = (current_dt - prev_dt).total_seconds()

        timestamps.append(timestamp_str)
        log_messages.append(message)
        statuses.append(new_label)
        products.append(current_product)
        product_ids.append(current_product_id)
        dates.append(file_name)
        time_diffs.append(time_diff)

        previous_timestamp = current_dt
        previous_label = new_label

    global_base_status = current_base_status

# ========== Build DataFrame ==========
df = pd.DataFrame({
    "Date": dates,
    "Timestamp": timestamps,
    "Log Message": log_messages,
    "Product": products,
    "Product_ID": product_ids,
    "Status": statuses,
    "Time Difference (Seconds)": time_diffs
})

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

print(df.head(2000))

# ✅ Export a middle slice (1M rows max) to Excel, change accordingly
excel_limit = 1_048_575
start_index = int(len(df) * 0.75)
df_slice = df.iloc[start_index:start_index + min(excel_limit, len(df) - start_index)]
output_path = r"C:\Users\shery\Downloads\SUTD\DBA\log_data_slice.xlsx"
df_slice.to_excel(output_path, index=False)
print(f"✅ Excel file with {len(df_slice)} rows saved to: {output_path}")

# ========== Generate Status Summary (No Assigned_ Columns) ==========
def generate_status_summary(df: pd.DataFrame, output_path: str):
    df_summary = df.copy()
    df_summary["Base_Status"] = df_summary["Status"].apply(base_status_of)
    df_summary["Time_Hours"] = df_summary["Time Difference (Seconds)"].apply(lambda x: x / 3600 if pd.notnull(x) else 0)

    summary = (
        df_summary.groupby(["Date", "Base_Status"])["Time_Hours"]
        .sum()
        .unstack(fill_value=0)
    )

    desired_order = ["Idle", "Standby", "Downtime", "Productive", "Off"]
    summary = summary.reindex(columns=desired_order, fill_value=0)
    summary.columns = [f"{col} (h)" for col in summary.columns]
    summary = summary.reset_index().round(2)
    summary.to_csv(output_path, index=False)
    print(f"✅ Daily status summary saved to: {output_path}")

# ========== Save Summary Output ==========
output_file = r"C:\Users\shery\Downloads\SUTD\DBA\status7.csv"
generate_status_summary(df, output_file)
