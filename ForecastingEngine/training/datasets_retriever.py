import subprocess
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
import openpyxl

# Function to run sqlcmd and fetch the data
def fetch_data():
    # SQL query to execute
    query = '''
        SELECT Timestamp, NodeName, CpuUsage, MemoryUsage
        FROM NodeMetrics
        ORDER BY Timestamp
    '''
    
    # SQLCMD command setup
    # You need to replace these with your actual server details
    sqlcmd_command = [
        'sqlcmd', 
        '-S', 'localhost,30001',        # Server and port
        '-U', 'SA',                     # Username
        '-P', '54879wMssql',            # Password
        '-d', 'MonitoringDB',           # Specify Database
        '-Q', query,                    # Query to execute
        '-s', ',',                      # Use comma as the delimiter
        '-W'                            # Remove extra spaces in the output
    ]

    try:
        # Run the command and capture output
        result = subprocess.run(sqlcmd_command, capture_output=True, text=True, check=True)
        
        # Get the output from the command
        output = result.stdout
        
        # Process the output into a DataFrame
        data = pd.read_csv(StringIO(output), delimiter=',')

        data.columns = data.columns.str.strip()

        return data
    
    except subprocess.CalledProcessError as e:
        print(f"Error running sqlcmd: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

# Function to resample data to 5-minute intervals and calculate the average
def resample_data(data, freq='5min'):
    if data.empty:
        print("No data to process.")
        return pd.DataFrame()

    # Set the timestamp as the index
    data['Timestamp'] = pd.to_datetime(data['Timestamp'], errors='coerce')
    
    # Drop rows with invalid timestamps
    data.dropna(subset=['Timestamp'], inplace=True)
    
    # Ensure all necessary columns are numeric (CpuUsage, MemoryUsage)
    data['CpuUsage'] = pd.to_numeric(data['CpuUsage'], errors='coerce')
    data['MemoryUsage'] = pd.to_numeric(data['MemoryUsage'], errors='coerce')

    # Drop rows with NaN values in these columns
    data.dropna(subset=['CpuUsage', 'MemoryUsage'], inplace=True)

    # Set the timestamp as index for resampling
    data.set_index('Timestamp', inplace=True)

    # Group by 'NodeName' and resample data in 5-minute intervals
    resampled_data = data.groupby('NodeName').resample(freq).mean()

    # Reset the index after resampling for better formatting
    resampled_data.reset_index(inplace=True)

    # Create 'item_id' as a combination of NodeName and metric type (cpu or memory)
    cpu_data = resampled_data[['Timestamp', 'NodeName', 'CpuUsage']].copy()
    cpu_data['item_id'] = cpu_data['NodeName'] + '_cpu'

    memory_data = resampled_data[['Timestamp', 'NodeName', 'MemoryUsage']].copy()
    memory_data['item_id'] = memory_data['NodeName'] + '_memory'

    # Combine CPU and memory data
    final_data = pd.concat([cpu_data[['Timestamp', 'NodeName', 'CpuUsage', 'item_id']].rename(columns={'CpuUsage': 'Value'}), 
                            memory_data[['Timestamp', 'NodeName', 'MemoryUsage', 'item_id']].rename(columns={'MemoryUsage': 'Value'})], ignore_index=True)

    # Filter out rows where 'Value' is NaN (in case of resampling gaps)
    final_data.dropna(subset=['Value'], inplace=True)

    # Drop 'NodeName' as per your requirement
    final_data.drop(columns=['NodeName'], inplace=True)

    return final_data

# Main function to execute the process
def main():
    # Fetch data from the database
    data = fetch_data()

    if data.empty:
        print("No data to process.")
        return

    # Resample and process the data
    resampled_data = resample_data(data)

    if not resampled_data.empty:
        # Insert the processed data into an Excel file
        resampled_data.to_excel('ClusterData.xlsx', index=False)
        print("Data processed and saved to 'ClusterData.xlsx' successfully!")
    else:
        print("Resampling produced no data.")

# Run the main function
if __name__ == '__main__':
    main()
