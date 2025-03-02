# DatabaseFunctions.py

import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os



def connect_to_database():
    try:
        # Create a connection string
        server = '135.148.27.67'
        database = 'PersonalFinance'
        username = 'brishty'
        password = 'DoNotShare$'
        driver = 'ODBC Driver 17 for SQL Server'
        
        connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
        engine = create_engine(connection_string)
        return engine
    except Exception as err:
        print("Error connecting to the database:", err)
        return None
    

def insert_dataframe_to_sql(df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Inserted DataFrame into table {table_name}")
    except Exception as e:
        print(f"Error inserting DataFrame into table {table_name}: {e}")


def run_stored_procedure(engine, procedure_name):
    with engine.connect() as connection:
        try:
            # Begin a transaction
            trans = connection.begin()
            try:
                # Construct the stored procedure call
                sp_call = text(f"EXEC {procedure_name}")
                connection.execute(sp_call)
                print(f"Executed stored procedure {procedure_name}")

                # Commit the transaction
                trans.commit()
            except Exception as e:
                # Rollback the transaction if there is an error
                trans.rollback()
                print(f"Error executing stored procedure {procedure_name}: {e}")
        except Exception as e:
            print(f"Error in transaction management: {e}")


def read_table_data(connection, table_name):
    try:
        query = f'SELECT * FROM {table_name}'
        return pd.read_sql(query, connection)
    except pyodbc.Error as err:
        print(f"Error executing SQL query on table {table_name}: {err}")
        return None
    

def run_query(engine, query):
    """
    Execute a SQL query. If it's a SELECT query, return the result as a pandas DataFrame.
    For other queries like UPDATE, INSERT, DELETE, return the number of affected rows.

    Parameters:
    engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine connected to the database.
    query (str): The SQL query to execute.

    Returns:
    pd.DataFrame or int: The result of the query as a DataFrame for SELECT queries,
                         or the number of affected rows for other queries.
    """
    try:
        with engine.connect() as connection:
            if query.strip().upper().startswith("SELECT"):
                result = pd.read_sql(query, connection)
                return result
            else:
                result = connection.execute(text(query))
                return result.rowcount
    except Exception as err:
        print(f"Error executing query: {err}")
        return None
            
            
def process_data(df, table_name, procedure_name=None):
    engine = connect_to_database()
    if engine is not None:
        insert_dataframe_to_sql(df, table_name, engine)
        if procedure_name is not None:
            run_stored_procedure(engine, procedure_name)
        with engine.connect() as connection:
            try:
                drop_temp_table = text(f"DROP TABLE IF EXISTS {table_name}")
                connection.execute(drop_temp_table)
                print(f"Dropped temporary table {table_name}")
            except Exception as e:
                print(f"Error dropping temporary table {table_name}: {e}")
    else:
        print("Failed to connect to the database.")            






import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def google_sheet_to_dataframe(url, sheet_name):
    """
    Convert a Google Sheet to a pandas DataFrame and stop processing when a blank column is encountered.
    
    Parameters:
    url (str): The URL of the Google Sheet.
    sheet_name (str): The name of the sheet to convert.
    
    Returns:
    pd.DataFrame: A DataFrame containing the data from the specified sheet of the Google Sheet.
    """
    # Define the scope and authenticate
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), 'GoogleSheetsAPICredentials.json')
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)

    try:
        # Open the Google Sheet by its URL
        sheet = client.open_by_url(url)

        # Select the sheet based on provided name
        worksheet = sheet.worksheet(sheet_name)  # Use sheet name

        # Get all values in the worksheet as a list of lists
        data = worksheet.get_all_values()

        # Find the index of the first blank column
        header = data[0]
        first_blank_col_index = len(header)  # Default to include all columns if no blank is found
        for i, col in enumerate(header):
            if col == "":  # Check for empty column name
                first_blank_col_index = i
                break

        # Only process up to the first blank column
        data_trimmed = [row[:first_blank_col_index] for row in data]

        # Convert to DataFrame
        df = pd.DataFrame(data_trimmed[1:], columns=data_trimmed[0])

        return df
    
    except gspread.exceptions.APIError as e:
        print(f"APIError: Check your Google API quotas and permissions. Details: {e}")
        return None

    except gspread.exceptions.SpreadsheetNotFound:
        print("Spreadsheet not found. Please ensure the URL is correct and the service account has access.")
        return None

    except PermissionError as e:
        print("PermissionError: Ensure the service account has been shared with the spreadsheet. Details:", e)
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def dataframe_to_google_sheet(df, url, sheet_name):
    """
    Export a pandas DataFrame to a specified Google Sheet and worksheet.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to export
    url (str): The URL of the Google Sheet
    sheet_name (str): The name of the sheet to write to
    
    Returns:
    bool: True if successful, False otherwise
    """
    # Define the scope and authenticate
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), 'GoogleSheetsAPICredentials.json')
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_PATH, scope)
    client = gspread.authorize(creds)

    try:
        # Open the Google Sheet by its URL
        sheet = client.open_by_url(url)
        
        # Select the worksheet or create it if it doesn't exist
        try:
            worksheet = sheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(sheet_name, rows=1, cols=1)
            print(f"Created new worksheet: {sheet_name}")

        # Clear the existing content
        worksheet.clear()
        
        # Convert DataFrame to list of lists
        data = [df.columns.values.tolist()] + df.values.tolist()
        
        # Update the worksheet
        worksheet.update(data)
        
        print(f"Successfully exported DataFrame to sheet: {sheet_name}")
        return True

    except gspread.exceptions.APIError as e:
        print(f"APIError: Check your Google API quotas and permissions. Details: {e}")
        return False
    
    except gspread.exceptions.SpreadsheetNotFound:
        print("Spreadsheet not found. Please ensure the URL is correct and the service account has access.")
        return False
    
    except PermissionError as e:
        print("PermissionError: Ensure the service account has been shared with the spreadsheet. Details:", e)
        return False
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return False
