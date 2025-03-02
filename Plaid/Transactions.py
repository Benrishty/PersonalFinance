import requests
import json
import pandas as pd
import DatabaseFunctions as dbf
from Configuration import PLAID_CLIENT_ID, PLAID_SECRET_KEY, PLAID_ENV, PLAID_WEBHOOK
import os
from datetime import datetime


def TransactionsSync(user_id='user123'):
    """Sync transactions for a specific user"""
    url = f"https://{PLAID_ENV}.plaid.com/transactions/sync"
    
    try:
        db_connection = dbf.connect_to_database()
        db_items = dbf.read_table_data(db_connection, 'Plaid_User_Items')
        db_items = db_items[db_items['Userid'] == user_id]
        print(db_items)

        if db_items.empty:
            print(f"No items found for user {user_id}")
            return

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

                # Check for errors in the response
                if 'error' in response_data:
                    print(f"Error in Plaid API response: {response_data['error']}")
                    break

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

                # Check if required keys exist
                if 'next_cursor' not in response_data or 'has_more' not in response_data:
                    print("Error: Missing required keys in response")
                    break

                # Update lastCursor and has_more for the next iteration
                lastCursor = response_data['next_cursor']
                has_more = response_data['has_more']

                cursor_data = {
                    'LastCursor': [lastCursor],
                    'access_token': [access_token]
                }
                cursor_df = pd.DataFrame(cursor_data)

                try:
                    dbf.insert_dataframe_to_sql(cursor_df, 'temp_cursor_data', db_connection)
                    dbf.run_stored_procedure(db_connection, 'SP_Update_Plaid_User_Items_LastCursor')
                except Exception as e:
                    print(f"Database error updating cursor: {e}")

                # Process added transactions
                if 'added' in response_data:
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
                        try:
                            dbf.insert_dataframe_to_sql(added_df, 'test_transactions_added', db_connection)
                        except Exception as e:
                            print(f"Database error inserting added transactions: {e}")
                    else:
                        print("No added transactions")

                # Process modified transactions
                if 'modified' in response_data:
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
                        try:
                            dbf.insert_dataframe_to_sql(modified_df, 'test_transactions_modified', db_connection)
                        except Exception as e:
                            print(f"Database error inserting modified transactions: {e}")
                    else:
                        print("No modified transactions")

                # Process removed transactions
                if 'removed' in response_data:
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
                        try:
                            dbf.insert_dataframe_to_sql(removed_df, 'test_transactions_removed', db_connection)
                        except Exception as e:
                            print(f"Database error inserting removed transactions: {e}")
                    else:
                        print("No removed transactions")
    except Exception as e:
        print(f"Error in TransactionsSync: {e}")


# Only run this code if the file is executed directly
if __name__ == "__main__":
    TransactionsSync()

