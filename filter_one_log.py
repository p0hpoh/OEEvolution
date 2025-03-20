import pandas as pd
import re
from pathlib import Path

# Define the file path (update this path to match your log file's location)
file_path = r"C:\Users\shery\Downloads\SUTD\DBA\combined_processed_log_data.csv"  # Update this path accordingly

# Read the file
with open(file_path, "r", encoding="latin1") as file:
    lines = file.readlines()

log_filename = file_path.split("/")[-1].replace(".log", "")

# Initialize variables
timestamps = []
log_messages = []
statuses = []
products = []
current_status = "standby"  # Default initial status
current_product = ""  # Default product name

# Flags to track active statuses
productive_active = False
idle_active = False
standby_active = False
downtime_active = False

# Define the list of patterns to filter by
patterns_to_keep = [
     r"----Start Procession: Manufacture----",
    r"\(0\)--Start Mark!-",
    r"\(0\)Stop PLC!",
    r"The Software Stop Button is Pressed",
    r"Software stopped unexpectedly",
    r"\(0\)Failed Waiting for PCB To Be in Place.*\(Err:32\)",
    r"\(0\)The Program is Pressed To Stop\(Err:32\)",
    r"Start processing failed: The Track System is Not Initialized\(Err:61\)",
    r"\(0\)Failed Waiting for PCB To Be in Place: Software stopped unexpectedly\(Err:32\)",
    r"SetFileName File: D:\\Production Program\\",
    r"\(0\)--Marking Completed\)",
    r"No Match Pattern Fool Proof!",
    r"Waiting for material to arrive failed: This Feature is Not Supported\(Err:48\)"
]

# Process each line in the log file
for line in lines:
    line = line.strip()
    if not line:
        continue
    
    # Extract timestamp and message
    match = re.match(r"(\d{2}:\d{2}:\d{2}):(.*)", line)
    if match:
        timestamp, message = match.groups()

        # Check if the message matches any of the patterns to keep
        if any(re.search(pattern, message) for pattern in patterns_to_keep):
            # Update product name if detected
            if "SetFileName File:" in message:
                current_product = message.split("SetFileName File:")[-1].strip()

            # Check for status transitions based on the filtered messages
            if "Start Mark" in message:
                current_status = "Productive"
                productive_active = True
                idle_active = standby_active = downtime_active = False
            elif "Successfully Cutting" in message:
                productive_active = False

            elif "(0)Stop PLC!" in message:
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

            # Append the filtered data
            timestamps.append(timestamp)
            log_messages.append(message)
            statuses.append(current_status)
            products.append(current_product)

# Create DataFrame
df = pd.DataFrame({
    "Date": log_filename,
    "Timestamp": timestamps,
    "Log Message": log_messages,
    "Product": products,
    "Status": statuses
})

# Define the output file path to the Downloads folder
output_file = Path("C:/Users/shery/Downloads/SUTD/DBA/filtered_log_data_remastered.csv")

# Save to CSV for reference
df.to_csv(output_file, index=False)

# Print message
print(f"Processed log data saved to {output_file}")

# Display the first few rows
print(df.head())