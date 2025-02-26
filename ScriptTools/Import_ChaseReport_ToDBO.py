import pandas as pd
import os
import shutil
from datetime import datetime
import DatabaseFunctions

print("Starting Chase CSV processing script...")

# Get current working directory and set up paths
current_dir = os.getcwd()
chase_dir = os.path.join(current_dir, "ChaseReports")
archive_dir = os.path.join(chase_dir, "archive")

print(f"\nCurrent working directory: {current_dir}")
print(f"Chase directory: {chase_dir}")
print(f"Archive directory: {archive_dir}")

# Verify chase_dir exists
if not os.path.exists(chase_dir):
    raise FileNotFoundError(f"Chase directory not found at: {chase_dir}\nPlease make sure you're running the script from the correct directory.")
    
# Create archive directory if it doesn't exist
if not os.path.exists(archive_dir):
    print(f"\nCreating archive directory at: {archive_dir}")
    os.makedirs(archive_dir, exist_ok=True)
else:
    print(f"\nArchive directory already exists at: {archive_dir}")

# Find all Chase CSV files in the directory
chase_files = [f for f in os.listdir(chase_dir) if f.endswith('.CSV') and f.startswith('Chase')]
print(f"\nFound {len(chase_files)} Chase CSV files:")
for file in chase_files:
    print(f"- {file}")

# Initialize an empty list to store DataFrames
dfs = []
print("\nProcessing files...")

# Process each file
for file in chase_files:
    file_path = os.path.join(chase_dir, file)
    print(f"\nProcessing: {file}")
    
    try:
        # Read the CSV file into a DataFrame
        print(f"Reading CSV file: {file}")
        df = pd.read_csv(file_path)
        
        # Add filename column and index
        df['Source_File'] = file
        df['Source_File_RowID'] = range(1, len(df) + 1)  # Add index starting from 1
        
        db_connection = DatabaseFunctions.connect_to_database()
        
        DatabaseFunctions.insert_dataframe_to_sql(df, 'temp_Report_ChaseCreditCardTransactions', db_connection)
        DatabaseFunctions.run_stored_procedure(db_connection, 'SP_Fill_Report_ChaseCreditCardTransactions')
        print(f"Successfully loaded DataFrame with shape: {df.shape}")
        dfs.append(df)
        
        # Move file to archive folder without changing the name
        archive_path = os.path.join(archive_dir, file)
        print(f"Moving file to archive: {file}")
        shutil.move(file_path, archive_path)
        print(f"Successfully archived file")
        
    except Exception as e:
        print(f"ERROR processing {file}: {str(e)}")

# Combine all DataFrames
if dfs:
    print("\nCombining all DataFrames...")
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"Successfully created combined DataFrame!")
    print(f"\nCombined DataFrame statistics:")
    print(f"- Total rows: {combined_df.shape[0]}")
    print(f"- Total columns: {combined_df.shape[1]}")
    print(f"- Date range: {combined_df['Transaction Date'].min()} to {combined_df['Transaction Date'].max()}")
    print(f"- Total amount: ${combined_df['Amount'].sum():.2f}")
    print(f"- Number of source files: {combined_df['Source_File'].nunique()}")
    
    print("\nFirst few rows of combined DataFrame:")
    print(combined_df.head())
    
    # Print unique source files
    print("\nSource files in DataFrame:")
    for source_file in combined_df['Source_File'].unique():
        file_count = len(combined_df[combined_df['Source_File'] == source_file])
        print(f"- {source_file}: {file_count} transactions")
    
else:
    print("\nNo CSV files were processed")

print("\nScript execution completed!")