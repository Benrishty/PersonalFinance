import os
import shutil
import pandas as pd
import DatabaseFunctions as dbf

# Directory containing csv files
folder_path = 'C:/Users/brishty/OneDrive - Bentex/Github/Dash2/DB Imports'
archive_path = os.path.join(folder_path, 'Archive')  # Archive folder within DB Imports

# Function to process each csv file
def process_csv_file(file_path):
    try:
        # Extract filename from file path
        filename = os.path.basename(file_path)
        print("This is the file: " + filename)
        
        # Read csv file into DataFrame
        df = pd.read_csv(file_path)
        
        # Insert DataFrame into SQL
        dbf.insert_dataframe_to_sql(df, filename, dbf.connect_to_database())
        print(f"Successfully processed {filename}")
        
        # Move file to archive folder after processing
        archive_file_path = os.path.join(archive_path, filename)
        shutil.move(file_path, archive_file_path)
        print(f"Moved {filename} to archive.")
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# Function to process each excel file
def process_excel_file(file_path):
    try:
        # Extract filename from file path
        filename = os.path.basename(file_path)
        print("This is the file: " + filename)
        
        # Read excel file into DataFrame
        df = pd.read_excel(file_path)
        
        # Insert DataFrame into SQL
        dbf.insert_dataframe_to_sql(df, filename, dbf.connect_to_database())
        print(f"Successfully processed {filename}")
        
        # Move file to archive folder after processing
        archive_file_path = os.path.join(archive_path, filename)
        shutil.move(file_path, archive_file_path)
        print(f"Moved {filename} to archive.")
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# Ensure the Archive folder exists
os.makedirs(archive_path, exist_ok=True)

# Loop through files in the directory
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):  # Adjust file extension as needed
        file_path = os.path.join(folder_path, filename)
        process_csv_file(file_path)
    if filename.endswith('.xlsx'):  # Adjust file extension as needed
        file_path = os.path.join(folder_path, filename)
        process_excel_file(file_path)
