import pandas as pd
import re
from pathlib import Path
from datetime import datetime, timedelta

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
assigned_bases = []
assigned_time_diffs = []

# ========== Global Variables ==========
last_product = ""
last_product_id = "99999999"
# We'll carry the last status from file to file; default is "Standby"
global_base_status = "Standby"
machine_powered_on = True

# ========== Utility Functions ==========
def base_status_of(label: str) -> str:
    """
    Convert "Start X" -> "X", "End X" -> "X".
    "Downtime" remains "Downtime", "Standby" remains "Standby".
    """
    if label.startswith("Start "):
        return label.replace("Start ", "")
    if label.startswith("End "):
        return label.replace("End ", "")
    return label

def compute_time_diff(prev_ts, current_ts, prev_label, current_label):
    """
    Compute time difference only if the base statuses match (ignoring "Start " / "End ")
    and are not "Downtime". 
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
    Retroactively mark the last appended line as "End X" if the old base was X
    in [Productive, Idle, Standby], ignoring "Downtime". 
    Does NOT forcibly revert to any fallback; that is handled by the main logic.
    """
    if statuses:  # If there's at least one row appended
        last_label = statuses[-1]
        last_base = base_status_of(last_label)
        # If the last base status is a real status (not downtime), end it
        if last_base in ["Productive", "Idle", "Standby"]:
            statuses[-1] = f"End {last_base}"

FALLBACK_STATUS = "Standby"  # If a status ends and no new status is triggered, fallback to "Standby"

# ========== Process Each File in Sorted Order ==========
for log_file in sorted(folder_path.glob("*.log")):
    file_name = log_file.stem
    file_date = current_date = datetime.today().date()

    with open(log_file, "r", encoding="latin-1", errors="ignore") as f:
        lines = f.readlines()
    if not lines:
        continue

    # We start this file with whatever global_base_status was set from the last file
    current_base_status = global_base_status
    previous_timestamp = None
    previous_label = current_base_status

    # Also keep the product info from previous usage
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

        # Possibly update product info
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
        backup_status = current_base_status  # old status
        new_label = backup_status  # default to old status
        clean_message = message.strip()

        # ===== Shutdown/Startup detection =====
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

        # ===== STATUS TRIGGERS =====

        # 1) "start mark" => end old status, then "Start Productive"
        elif  "(0)--start mark!--" in line_lower:
            # End old status if it's not downtime
            if backup_status != "Downtime":
                end_previous_status()
            new_label = "Start Productive"
            current_base_status = "Productive"

        # 2) "successfully cutting" => line itself is "End Productive"
        elif "successfully cutting" in line_lower and backup_status == "Productive":
            # Do NOT call end_previous_status() -> 
            # we want THIS line to be "End Productive"
            new_label = "End Productive"
            # after ending, fallback to the old status or to "Standby"?
            current_base_status = FALLBACK_STATUS

        # 3) "(0)stop plc!" => end old status, then "Start Idle"
        elif ( "(0)stop plc!" in line_lower or "the software stop button is pressed" in line_lower):
            if backup_status != "Downtime":
                end_previous_status()
            new_label = "Start Idle"
            current_base_status = "Idle"

        # 4) "alarm reset" => line itself is "End Idle"
        elif "alarm" in line_lower and "reset" in line_lower and backup_status == "Idle":
            # do NOT call end_previous_status() => 
            # we want this line to be "End Idle"
            new_label = "End Idle"
            # after ending, fallback
            current_base_status = FALLBACK_STATUS

        else:
            # Possibly "start procession: manufacture" => end old status, start Standby
            temp = line_lower.strip("- ").strip()
            if "start procession" in temp and "manufacture" in temp:
                if backup_status != "Downtime":
                    end_previous_status()
                new_label = "Start Standby"
                current_base_status = "Standby"
            # "err:" => single-line downtime
            elif "err:" in line_lower:
                new_label = "Downtime"
                # revert to old status after line
                current_base_status = backup_status
            else:
                # no triggers => remain in old status
                new_label = backup_status

        # ===== Time Difference =====
        if machine_powered_on and previous_timestamp and previous_label != "Off":
            prev_full = datetime.combine(file_date, previous_timestamp.time())
            curr_full = datetime.combine(current_date, current_dt.time())
            time_diff = (curr_full - prev_full).total_seconds()

            midnight = datetime.combine(prev_full.date() + timedelta(days=1), datetime.min.time())

            if prev_full < midnight < curr_full:
                seconds_before_midnight = (midnight - prev_full).total_seconds()
                seconds_after_midnight = (curr_full - midnight).total_seconds()

                assigned_bases.append(base_status_of(previous_label))
                assigned_time_diffs.append(seconds_before_midnight)
                dates.append(prev_full.strftime("%d/%m/%Y"))

                assigned_bases.append(base_status_of(previous_label))
                assigned_time_diffs.append(seconds_after_midnight)
                dates.append(curr_full.strftime("%d/%m/%Y"))

                time_diffs.append(seconds_before_midnight + seconds_after_midnight)
            else:
                assigned_bases.append(base_status_of(previous_label))
                assigned_time_diffs.append(time_diff)
                dates.append(prev_full.strftime("%d/%m/%Y"))
                time_diffs.append(time_diff)
        else:
            assigned_bases.append(None)
            assigned_time_diffs.append(None)
            dates.append(file_date.strftime("%d/%m/%Y"))
            time_diffs.append(None)

        # ===== Append row =====
        timestamps.append(timestamp_str)
        log_messages.append(message)
        statuses.append(new_label)
        products.append(current_product)
        product_ids.append(current_product_id)

        previous_timestamp = current_dt
        previous_label = new_label

    # After finishing a file, update global_base_status so next file 
    # starts with the same status
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

df["Assigned_Base"] = assigned_bases
df["Assigned_Time_Seconds"] = assigned_time_diffs
df["Assigned_Time_Hours"] = df["Assigned_Time_Seconds"] / 3600

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
    print(f"✅ Daily status summary saved to: {output_path}")

# ========== Save Output ==========
output_file = r"C:\Users\shery\Downloads\SUTD\DBA\status7.csv"
generate_status_summary(df, output_file)