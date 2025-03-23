import pandas as pd
import re
from pathlib import Path
from datetime import datetime, timedelta

# ========== CONFIG ==========
# folder_path = Path("/Users/sabaiyi/Desktop/SUTD/term4/DBA/project/2024 Logs/PcbVision/PCB/Log/test")
folder_path = Path(r"C:\Users\Jerald\Documents\Uni Docs\Term 4\Data Business and Analytics\Project\Datasets\2024 Logs\PcbVision\PCB\Log\Machine")

# ========== DataFrame Lists ==========
timestamps = []
log_messages = []
statuses = []        # Use this list for status labels
products = []
product_ids = []
time_diffs = []
dates = []

# ========== Global Variables ==========
last_product = ""
last_product_id = "99999999"
# We'll carry the last status from file to file; default is "Idle"
global_base_status = "Standby"
FALLBACK_STATUS = "Standby"  # fallback if a status ends and no new trigger appears

# ========== Utility Functions ==========
def base_status_of(label: str) -> str:
    """
    Convert "Start X" -> "X", "End X" -> "X".
    "Downtime", "Standby", "Off" remain as is.
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
    if prev_base == this_base and prev_base != "Downtime":
        return (current_ts - prev_ts).total_seconds()
    return None

def end_previous_status(entries, including_downtime=False):
    """
    Retroactively mark the last appended line as "End X" if the old base was X
    in [Productive, Idle, Standby, Off]. 
    If including_downtime is True, also end Downtime.
    (This function is used only when starting a new multi-line status.)
    """
    if entries:
        last_label = entries[-1]["Status"]
        last_base = base_status_of(last_label)
        valid_statuses = ["Productive", "Idle", "Standby", "Off"] if including_downtime else ["Productive", "Idle", "Standby"]
        if last_base in valid_statuses:
            entries[-1]["Status"] = f"End {last_base}"

# ========== Process Each File in Sorted Order ==========
# We'll collect log entries as a list of dictionaries.
log_entries = []

for log_file in sorted(folder_path.glob("*.log")):
    file_name = log_file.stem

    with open(log_file, "r", encoding="latin-1", errors="ignore") as f:
        lines = f.readlines()
    if not lines:
        continue

    # Start this file with the global base status from previous file.
    current_base_status = global_base_status
    previous_timestamp = None
    previous_label = current_base_status

    # Retain product info from previous files.
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

        # Update product info if present.
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
        backup_status = current_base_status  # store the current status before applying any trigger
        new_label = backup_status  # default is to remain in the current status

        # ===== TRIGGERS =====

        # (A) "****************Close Software***************"
        if "****************close software***************" in line_lower:
            # For a one-line Off status, we do not call end_previous_status()
            new_label = "Off"
            current_base_status = "Off"

        # (B) "|*************Start PCB*************|"
        elif "|*************start pcb*************|" in line_lower:
            # Only update to Standby if the previous line's base status is Off.
            if log_entries and base_status_of(log_entries[-1]["Status"]) == "Off":
                new_label = "Standby"
                current_base_status = "Standby"
            else:
                new_label = backup_status

        # (C) (0)--start mark!-- => end previous status then start Productive
        elif "(0)--start mark!--" in line_lower:
            if backup_status != "Downtime":
                end_previous_status(log_entries)
            new_label = "Start Productive"
            current_base_status = "Productive"

        # (D) "successfully cutting" => current line is "End Productive"
        elif "successfully cutting" in line_lower and backup_status == "Productive":
            new_label = "End Productive"
            current_base_status = FALLBACK_STATUS

        # (E) (0)stop plc! or "The Software Stop Button is Pressed" => end previous status then start Idle
        elif "(0)stop plc!" in line_lower or "the software stop button is pressed" in line_lower:
            if backup_status != "Downtime":
                end_previous_status(log_entries)
            new_label = "Start Idle"
            current_base_status = "Idle"

        # (F) "alarm reset" => current line is "End Idle"
        elif "alarm" in line_lower and "reset" in line_lower and backup_status == "Idle":
            new_label = "End Idle"
            current_base_status = FALLBACK_STATUS

        # (G) "----start procession: manufacture----" => end previous status then start Standby
        elif "start procession" in line_lower and "manufacture" in line_lower:
            if backup_status != "Downtime":
                end_previous_status(log_entries)
            new_label = "Start Standby"
            current_base_status = "Standby"

        # (H) "err:" => single-line downtime; do not end previous status
        elif "err:" in line_lower:
            new_label = "Downtime"
            current_base_status = backup_status
        else:
            new_label = backup_status

        # Append the log entry along with parsed timestamp for forward time diff calculation.
        log_entries.append({
            "Date": file_name,
            "Timestamp": timestamp_str,
            "Log Message": message.strip(),
            "Product": current_product,
            "Product_ID": current_product_id,
            "Status": new_label,
            "Parsed_TS": current_dt
        })

        previous_timestamp = current_dt
        previous_label = new_label

    # Update global_base_status to carry over the status to the next file.
    global_base_status = current_base_status

# ========== Build DataFrame ==========
df = pd.DataFrame(log_entries)

# ========== Compute Forward Time Differences ==========
df["Next_TS"] = df["Parsed_TS"].shift(-1)
df["Time Difference (Seconds)"] = (df["Next_TS"] - df["Parsed_TS"]).dt.total_seconds().clip(lower=0).fillna(0)
df.drop(columns=["Parsed_TS", "Next_TS"], inplace=True)



# ========== Changes number of rows Pandas shows ==========
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)

print(df.head(2000))



# ========== Function to Produce Product Name ID Table ==========
def extract_unique_products_from_df(df):
    """
    Given a DataFrame with a 'Log Message' column, extract unique product entries
    where Product ID must be exactly 8 digits and appear between '\' and '_'.
    Returns a new DataFrame with 'Original Line', 'Product Name', and 'Product_ID'.
    """
    product_names = []
    product_ids = []
    original_lines = []
    seen_products = set()

    for line in df["Log Message"]:
        line = line.strip()
        if not line or "SetFileName" not in line:
            continue

        current_product = line.split("SetFileName File:")[-1].strip()
        current_product_name = current_product.split("\\")[-1]

        # ✅ Strictly match only 8 digits BETWEEN \ and _
        product_id_match = re.search(r"\\(\d{8})_", current_product)
        if product_id_match:
            current_product_id = product_id_match.group(1)
        else:
            current_product_id = "99999999"

        product_key = (current_product_name, current_product_id)
        if product_key in seen_products:
            continue
        seen_products.add(product_key)

        product_names.append(current_product_name)
        product_ids.append(current_product_id)
        original_lines.append(line)

    result_df = pd.DataFrame({
        "Original Line": original_lines,
        "Product Name": product_names,
        "Product_ID": product_ids
    }).drop_duplicates().sort_values(by="Product_ID").reset_index(drop=True)

    return result_df



# ========== Function to Produce Number of Products Table ==========
def extract_number_of_products_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts a table that summarizes product marking cycles from a detailed log DataFrame.
    This includes timing information for each 'Start Mark!' cycle, number of units, and ideal cycle times.
    """
    # Convert Timestamp to datetime format (if not already)
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], format="%H:%M:%S")

    # Initialize list for storing extracted cycle data
    cycles = []

    # Iterate through the dataframe to extract cycles
    i = 0
    while i < len(df):
        # Find the "Start Mark!" log to begin a cycle
        if "Start Mark!" in df.iloc[i]["Log Message"]:
            start_time = df.iloc[i]["Timestamp"]
            date = df.iloc[i]["Date"]
            product_id = df.iloc[i]["Product_ID"]
            i += 1
            marking_count = 0
            end_time = None

            # Look for the corresponding "Successfully Cutting" or "Stop PLC!" log
            while i < len(df):
                log_message = df.iloc[i]["Log Message"]

                if "Successfully Cutting" in log_message:
                    end_time = df.iloc[i]["Timestamp"]
                    break

                if re.search(r"\(0\)Marking Completed\(\d+ms\)", log_message):
                    marking_count += 1

                if "Stop PLC!" in log_message and end_time is None:
                    if marking_count > 0:
                        end_time = df.iloc[i]["Timestamp"]
                    break

                i += 1

            if end_time and end_time > start_time:
                cycle_duration = (end_time - start_time).total_seconds()
                unit_duration = cycle_duration / marking_count if marking_count > 0 else None

                cycles.append([
                    date,
                    product_id,
                    start_time.time(),
                    end_time.time(),
                    cycle_duration,
                    marking_count,
                    unit_duration
                ])

        i += 1

    result_df = pd.DataFrame(
        cycles,
        columns=[
            "Date", "Product_ID", "Cycle_Start_Time", "Cycle_End_Time",
            "Cycle_Duration", "Number_of_Units", "Unit_Duration"
        ]
    )

    # ---- Add Ideal Unit Time and Ideal Cycle Time ----
    ideal_unit_time = result_df.groupby("Product_ID")["Unit_Duration"]\
        .apply(lambda x: x[x > 0][x[x > 0] <= x[x > 0].quantile(0.25)].min())

    result_df["Ideal_Unit_Time"] = result_df["Product_ID"].map(ideal_unit_time)
    result_df["Ideal_Cycle_Time"] = result_df["Ideal_Unit_Time"] * result_df["Number_of_Units"]

    return result_df



# ✅ Export a middle slice (1M rows max) to Excel, change accordingly
excel_limit = 1_048_575
start_index = int(len(df) * 0.75)
df_slice = df.iloc[start_index:start_index + min(excel_limit, len(df) - start_index)]
output_path = r"C:\Users\Jerald\Downloads\Dataframe_5_Columns_Base.xlsx"
df_slice.to_excel(output_path, index=False)
print(f"✅ Excel file with {len(df_slice)} rows saved to: {output_path}")

# ========== Export Excel file for Product Name/ID Table ==========
product_table = extract_unique_products_from_df(df)
product_table.to_excel(r"C:\Users\Jerald\Downloads\Product_Name_ID_Table.xlsx", index=False)
print("✅ Product table exported successfully.")

# ========== Export Excel file for Number of Products Table ==========
Number_of_Products_df = extract_number_of_products_table(df)
Number_of_Products_df.to_excel(r"C:\Users\Jerald\Downloads\Number_of_Products_Table.xlsx", index=False)
print("✅ Number of Products table exported successfully.")

