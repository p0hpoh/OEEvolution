import pandas as pd
import re
from pathlib import Path

# Define the folder path (update accordingly)
#folder_path = Path("/Users/sabaiyi/Desktop/SUTD/term4/DBA/project/2024 Logs/PcbVision/PCB/Log/Machine")
folder_path = Path(r"C:\Users\Jerald\Documents\Uni Docs\Term 4\Data Business and Analytics\Project\Datasets\2024 Logs\PcbVision\PCB\Log\Machine")

# Initialize lists to store data across all files
timestamps = []
log_messages = []
statuses = []
products = []
dates = []

# Process each log file in the folder
for log_file in folder_path.glob("*.log"):
    log_filename = log_file.stem  # Extract the filename without extension

    # Read the file
    with open(log_file, "r", encoding="latin-1", errors="ignore") as file:
        lines = file.readlines()

    # Initialize status tracking variables
    current_status = "Idle"  # Default status
    current_product = ""  # Default product name
    productive_active = False
    idle_active = False
    standby_active = False
    downtime_active = False

    # Process each line in the log file
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Extract timestamp and message
        match = re.match(r"(\d{2}:\d{2}:\d{2}):(.*)", line)
        if match:
            timestamp, message = match.groups()

            # Update product name if detected
            if "SetFileName File:" in message:
                current_product = message.split("SetFileName File:")[-1].strip()

            # Check for status transitions
            if "(0)--Start Mark!--" in message:
                current_status = "Productive"
                productive_active = True
                idle_active = standby_active = downtime_active = False
            elif "Successfully Cutting" in message:
                productive_active = False

            elif "Stop PLC!" in message:
                current_status = "Idle"
                idle_active = True
                productive_active = standby_active = downtime_active = False

            elif "The Software Stop Button is Pressed" in message:
                current_status = "Idle"
                idle_active = True
                productive_active = standby_active = downtime_active = False

            elif "Alarm reset" in message:
                idle_active = False

            elif "----Start Procession: Manufacture----" in message:
                current_status = "Standby"
                standby_active = True
                productive_active = idle_active = downtime_active = False

            elif "Start Mark" in message and standby_active:
                standby_active = False

            elif "Err:" in message:
                current_status = "Downtime"
                downtime_active = True
                productive_active = idle_active = standby_active = False
            else:
                # Maintain the current active status
                if productive_active:
                    current_status = "Productive"
                elif idle_active:
                    current_status = "Idle"
                elif standby_active:
                    current_status = "Standby"
                elif downtime_active:
                    current_status = "Downtime"

            # Append processed data for this line
            timestamps.append(timestamp)
            log_messages.append(message)
            statuses.append(current_status)
            products.append(current_product)
            dates.append(log_filename)  # Store the filename as the "Date" column

# Create DataFrame
df = pd.DataFrame({
    "Date": dates,
    "Timestamp": timestamps,
    "Log Message": log_messages,
    "Product": products,
    "Status": statuses
})

# Save to CSV for reference
#output_file = "combined_processed_log_data.csv"
#df.to_csv(output_file, index=False)

# Print message
#print(f"Processed log data saved to {output_file}")

# Stops panda from truncating / summarising all the rows of data into first 5
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns (if needed)
pd.set_option('display.expand_frame_repr', False)  # Prevents cutting off long tables

# Display the first few rows
print(df.head(1000))
