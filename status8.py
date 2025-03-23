import pandas as pd
import re
from pathlib import Path
from datetime import datetime, timedelta

# ========== CONFIG ==========
# Set the folder containing the log files
folder_path = Path(r"C:\Users\shery\Downloads\SUTD\DBA\2024 Logs\PcbVision\PCB\Log\Machine")

# ========== DataFrame Lists ==========
# These will be used to build the final DataFrame
timestamps = []
log_messages = []
statuses = []
products = []
product_ids = []
time_diffs = []
dates = []
assigned_bases = []
assigned_time_diffs = []

# ========== Global Variables ==========
last_product = ""
last_product_id = "99999999"
global_base_status = "Standby"  # Carries status across files
machine_powered_on = True

# ========== Utility Functions ==========
def base_status_of(label: str) -> str:
    """
    Simplifies status labels:
    "Start Productive" -> "Productive", etc.
    """
    if label.startswith("Start "):
        return label.replace("Start ", "")
    if label.startswith("End "):
        return label.replace("End ", "")
    return label

def compute_time_diff(prev_ts, current_ts, prev_label, current_label):
    """
    Calculates time difference between timestamps if base status matches
    and is not a downtime/off period.
    """
    if prev_ts is None:
        return None
    prev_base = base_status_of(prev_label)
    this_base = base_status_of(current_label)
    if prev_base == this_base and prev_base not in ["Downtime", "Off"]:
        return (current_ts - prev_ts).total_seconds()
    return None

def end_previous_status():
    """
    Retroactively ends the last active status by rewriting it as "End X".
    Only applies to meaningful states.
    """
    if statuses:
        last_label = statuses[-1]
        last_base = base_status_of(last_label)
        if last_base in ["Productive", "Idle", "Standby"]:
            statuses[-1] = f"End {last_base}"

FALLBACK_STATUS = "Standby"  # Default state after an action ends

# ========== Process Each File in Sorted Order ==========
for log_file in sorted(folder_path.glob("*.log")):
    file_name = log_file.stem

    # ✅ Parse date from filenames like "2024.3.9" or "2024.12.25"
    date_match = re.search(r"(\d{4})\.(\d{1,2})\.(\d{1,2})", file_name)
    if date_match:
        y, m, d = date_match.groups()
        file_date = current_date = datetime(int(y), int(m), int(d)).date()
    else:
        print(f"\u26a0\ufe0f Could not parse date from filename: {file_name}")
        continue

    # Read all lines from the log file
    with open(log_file, "r", encoding="latin-1", errors="ignore") as f:
        lines = f.readlines()
    if not lines:
        continue

    # Initialize state per file
    current_base_status = global_base_status
    previous_timestamp = None
    previous_label = current_base_status
    current_product = last_product
    current_product_id = last_product_id

    # ========== Process Each Line ==========
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        # Match timestamped lines: "HH:MM:SS: message"
        match = re.match(r"(\d{2}:\d{2}:\d{2}):(.*)", line)
        if not match:
            continue

        timestamp_str, message = match.groups()
        current_dt = datetime.strptime(timestamp_str, "%H:%M:%S")

        # Extract product ID from filename line
        if "SetFileName File:" in message:
            current_product = message.split("SetFileName File:")[-1].strip()
            last_product = current_product
            pid_match = re.search(r"[/\\](\d{8})[_-]", current_product)
            if pid_match:
                current_product_id = pid_match.group(1)
                last_product_id = current_product_id
            else:
                current_product_id = "99999999"

        # Default status handling
        line_lower = message.lower()
        backup_status = current_base_status
        new_label = backup_status

        # === Status change detection ===
        clean_message = message.strip()

        if clean_message == "**************Close Software**************":
            if backup_status not in ["Downtime", "Off"]:
                end_previous_status()
            new_label = "Off"
            current_base_status = "Off"
            machine_powered_on = False

        elif clean_message == "|*************Start PCB*************|":
            new_label = FALLBACK_STATUS
            current_base_status = FALLBACK_STATUS
            machine_powered_on = True

        elif "(0)--start mark!--" in line_lower:
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

        # === Time Difference Handling ===
        if machine_powered_on and previous_timestamp and previous_label != "Off":
            prev_full = datetime.combine(file_date, previous_timestamp.time())
            curr_full = datetime.combine(current_date, current_dt.time())
            time_diff = (curr_full - prev_full).total_seconds()

            midnight = datetime.combine(prev_full.date() + timedelta(days=1), datetime.min.time())

            if prev_full < midnight < curr_full:
                # Split across midnight
                before = (midnight - prev_full).total_seconds()
                after = (curr_full - midnight).total_seconds()

                assigned_bases.extend([base_status_of(previous_label)] * 2)
                assigned_time_diffs.extend([before, after])
                dates.extend([
                    prev_full.strftime("%d/%m/%Y"),
                    curr_full.strftime("%d/%m/%Y")
                ])

                time_diffs.append(before + after)
            else:
                assigned_bases.append(base_status_of(previous_label))
                assigned_time_diffs.append(time_diff)
                dates.append(prev_full.strftime("%d/%m/%Y"))
                time_diffs.append(time_diff)
        else:
            # No time diff possible
            assigned_bases.append(None)
            assigned_time_diffs.append(None)
            dates.append(file_date.strftime("%d/%m/%Y"))
            time_diffs.append(None)

        # === Append to main lists ===
        timestamps.append(timestamp_str)
        log_messages.append(message)
        statuses.append(new_label)
        products.append(current_product)
        product_ids.append(current_product_id)

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
    "Status": statuses
})

# ✅ Add time assignment columns
df["Assigned_Base"] = assigned_bases
df["Assigned_Time_Seconds"] = assigned_time_diffs
df["Assigned_Time_Hours"] = df["Assigned_Time_Seconds"].apply(lambda x: x / 3600 if pd.notnull(x) else None)

# ========== Display Sample ==========
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

print(df.head(2000))

# ========== Generate Status Summary ==========
def generate_status_summary(df: pd.DataFrame, output_path: str):
    df_summary = df.copy()
    df_summary = df_summary[
        (df_summary["Assigned_Base"].notnull()) & 
        (df_summary["Assigned_Time_Seconds"].notnull())
    ]
    summary = (
        df_summary.groupby(["Date", "Assigned_Base"])["Assigned_Time_Hours"]
        .sum()
        .unstack(fill_value=0)
    )
    desired_order = ["Idle", "Standby", "Downtime", "Productive", "Off"]
    summary = summary.reindex(columns=desired_order, fill_value=0)
    summary.columns = [f"{col} (h)" for col in summary.columns]
    summary = summary.reset_index().round(2)
    summary.to_csv(output_path, index=False)
    print(f"\u2705 Daily status summary saved to: {output_path}")

# ========== Save Output ==========
output_file = r"C:\Users\shery\Downloads\SUTD\DBA\status7.csv"
generate_status_summary(df, output_file)

