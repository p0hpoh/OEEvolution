import pandas as pd

# Path to the CSV file with the processed log data
csv_file_path = r"C:\Users\shery\Downloads\SUTD\DBA\log_data_slice.csv"  # Update with your actual file path

# Load the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)

# Assuming the columns are properly named (Date, Timestamp, Log Message, Product, Product_ID, Status)
df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%H:%M:%S')
df['Date'] = pd.to_datetime(df['Date'], format='%Y.%m.%d')

# Define time difference per status change per entry (assuming equal distribution)
df['Time_Diff'] = df.groupby('Date')['Timestamp'].diff().fillna(pd.Timedelta(seconds=0))

# Convert time differences to hours
df['Time_Diff_Hours'] = df['Time_Diff'].dt.total_seconds() / 3600

# Summarize total time spent per status per day
status_summary = df.groupby(['Date', 'Status'])['Time_Diff_Hours'].sum().unstack(fill_value=0)

# Rename columns to match the required format
status_summary = status_summary.rename(columns={'Idle': 'Idle (h)', 'Standby': 'Standby (h)', 
                                                'Downtime': 'Downtime (h)', 'Productive': 'Productive (h)'})

# Reset index for final display
status_summary = status_summary.reset_index()

# Save to CSV
output_path = r"C:\Users\shery\Downloads\SUTD\DBA\status_summary.csv"  # Update with your desired output path
status_summary.to_csv(output_path, index=False)

print(f"✅ Status summary table saved to: {output_path}")
