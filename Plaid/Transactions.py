import requests
import json
import pandas as pd
import DatabaseFunctions as dbf
from Configuration import PLAID_CLIENT_ID, PLAID_SECRET_KEY, PLAID_ENV, PLAID_WEBHOOK
import os
from datetime import datetime



def TransactionsSync():
    """Create a public token for a specific user"""
    url = f"https://{PLAID_ENV}.plaid.com/transactions/sync"
    
    user_id = 'user123'

    db_connection = dbf.connect_to_database()
    db_items = dbf.read_table_data(db_connection, 'Plaid_User_Items')
    db_items = db_items[db_items['Userid'] == user_id]
    print(db_items)

    for index, item in db_items.iterrows():
        access_token = item['access_token']
        
        lastCursor = item.get('LastCursor', None)

        has_more = True
        while has_more:
            payload = json.dumps({
                "client_id":  PLAID_CLIENT_ID,
                "secret": PLAID_SECRET_KEY,
                "access_token": access_token,
                "cursor": lastCursor,
                "count": 500
            })
            headers = {
                'Content-Type': 'application/json'
            }

            response = requests.request("POST", url, headers=headers, data=payload)
            print('printing response for access token:', access_token)
            response_data = response.json()

            # Create a directory for storing JSON responses if it doesn't exist
            json_dir = "plaid_responses"
            os.makedirs(json_dir, exist_ok=True)
            
            # Generate a filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{json_dir}/transaction_sync_{user_id}_{timestamp}.json"
            
            # Save the raw JSON response to a file
            with open(filename, "w") as f:
                json.dump(response_data, f, indent=4)
            
            print(f"Raw JSON response saved to {filename}")

            # Update lastCursor and has_more for the next iteration
            lastCursor = response_data['next_cursor']
            has_more = response_data['has_more']

            cursor_data = {
                'LastCursor': [lastCursor],
                'access_token': [access_token]
            }
            cursor_df = pd.DataFrame(cursor_data)

            dbf.insert_dataframe_to_sql(cursor_df, 'temp_cursor_data', db_connection)
            dbf.run_stored_procedure(db_connection, 'SP_Update_Plaid_User_Items_LastCursor')

            # Process added transactions
            added_transactions = []
            for transaction in response_data['added']:
                # Extract just the primary category if personal_finance_category exists
                personal_finance_category = None
                if transaction.get('personal_finance_category') and 'primary' in transaction.get('personal_finance_category'):
                    personal_finance_category = transaction.get('personal_finance_category')['primary']
                
                added_transactions.append({
                    'transaction_id': transaction['transaction_id'],
                    'userID': user_id,
                    'account_id': transaction['account_id'],
                    'personal_finance_category': personal_finance_category,
                    'date': transaction['date'],
                    'authorized_date': transaction.get('authorized_date'),
                    'merchant_name': transaction.get('merchant_name'),
                    'amount': transaction['amount'],
                    'iso_currency_code': transaction['iso_currency_code'],
                    'pending_transaction_id': transaction.get('pending_transaction_id')
                })

            # Create DataFrame for added transactions
            if added_transactions:
                added_df = pd.DataFrame(added_transactions)
                print("Added Transactions:")
                print(added_df.head())
                dbf.insert_dataframe_to_sql(added_df, 'test_transactions_added', db_connection)
            else:
                print("No added transactions")

            # Process modified transactions
            modified_transactions = []
            for transaction in response_data['modified']:
                # Extract just the primary category if personal_finance_category exists
                personal_finance_category = None
                if transaction.get('personal_finance_category') and 'primary' in transaction.get('personal_finance_category'):
                    personal_finance_category = transaction.get('personal_finance_category')['primary']
                
                modified_transactions.append({
                    'transaction_id': transaction['transaction_id'],
                    'userID': user_id,
                    'account_id': transaction['account_id'],
                    'personal_finance_category': personal_finance_category,
                    'date': transaction['date'],
                    'authorized_date': transaction.get('authorized_date'),
                    'merchant_name': transaction.get('merchant_name'),
                    'amount': transaction['amount'],
                    'iso_currency_code': transaction['iso_currency_code'],
                    'pending_transaction_id': transaction.get('pending_transaction_id')
                })

            # Create DataFrame for modified transactions
            if modified_transactions:
                modified_df = pd.DataFrame(modified_transactions)
                print("\nModified Transactions:")
                print(modified_df.head())
                dbf.insert_dataframe_to_sql(modified_df, 'test_transactions_modified', db_connection)
            else:
                print("No modified transactions")

            # Process removed transactions
            removed_transactions = []
            for transaction in response_data['removed']:
                removed_transactions.append({
                    'transaction_id': transaction['transaction_id'],
                    'userID': user_id,
                    'account_id': transaction['account_id']
                })

            # Create DataFrame for removed transactions
            if removed_transactions:
                removed_df = pd.DataFrame(removed_transactions)
                print("\nRemoved Transactions:")
                print(removed_df.head())
                dbf.insert_dataframe_to_sql(removed_df, 'test_transactions_removed', db_connection)
            else:
                print("No removed transactions")


TransactionsSync()

