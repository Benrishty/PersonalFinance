import requests
import json
import pandas as pd
import DatabaseFunctions as dbf
from Configuration import PLAID_CLIENT_ID, PLAID_SECRET_KEY, PLAID_ENV, PLAID_WEBHOOK



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
        
        if 'LastCursor' in item:
            lastCursor = item['LastCursor']
        else:
            lastCursor = None  # or some default value

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
        # print(response.text)
        response_data = response.json()

        cursor_data = {
            'LastCursor': [response_data['next_cursor']],
            'access_token': [access_token]
        }
        cursor_df = pd.DataFrame(cursor_data)
        dbf.insert_dataframe_to_sql(cursor_df, 'temp_cursor_data', db_connection)
        dbf.run_stored_procedure(db_connection, 'SP_Update_Plaid_User_Items_LastCursor')

        # Save the response to a JSON file
        # with open(f'transactions_{index}.json', 'w') as json_file:
        #     json.dump(response.json(), json_file, indent=4)

        # Function to create a DataFrame for added and modified transactions
        def create_transaction_df(transactions, fields):
            if transactions:
                df = pd.json_normalize(transactions)
                return df[fields]
            else:
                return pd.DataFrame(columns=fields)

        # Fields for added and modified transactions
        transaction_fields = [
            'transaction_id',
            'account_id',
            'amount',
            'date',
            'merchant_name',
            'category',
            'category_id',
            'payment_channel',
            'authorized_date',
            'location.city',
            'location.region',
            'location.country'
        ]

        # Create DataFrame for added transactions
        added_transactions = response_data.get('added', [])
        added_df = create_transaction_df(added_transactions, transaction_fields)
        print("Added Transactions:")
        print(added_df.head())
        dbf.insert_dataframe_to_sql(added_df, 'test_transactions_added', db_connection)

        # Create DataFrame for modified transactions
        modified_transactions = response_data.get('modified', [])
        modified_df = create_transaction_df(modified_transactions, transaction_fields)
        print("\nModified Transactions:")
        print(modified_df.head())
        dbf.insert_dataframe_to_sql(modified_df, 'test_transactions_modified', db_connection)

        # Fields for removed transactions
        removed_fields = [
            'transaction_id',
            'account_id'
        ]

        # Create DataFrame for removed transactions
        removed_transactions = response_data.get('removed', [])
        if removed_transactions:
            removed_df = pd.json_normalize(removed_transactions)
            removed_df = removed_df[removed_fields]
        else:
            removed_df = pd.DataFrame(columns=removed_fields)

        print("\nRemoved Transactions:")
        print(removed_df.head())
        dbf.insert_dataframe_to_sql(removed_df, 'test_transactions_removed', db_connection)


TransactionsSync()

