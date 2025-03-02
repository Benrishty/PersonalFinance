import requests
import json
import pandas as pd
import DatabaseFunctions as dbf
from Configuration import PLAID_CLIENT_ID, PLAID_SECRET_KEY, PLAID_ENV, PLAID_WEBHOOK


def createItem_public_token():
    """Create a public token for a specific user"""
    url = f"https://{PLAID_ENV}.plaid.com/sandbox/public_token/create"
    
    payload = json.dumps({
        "client_id": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET_KEY,
        "institution_id": "ins_20",
        "initial_products": [
            "transactions"
        ],
        "options": {
            "webhook": PLAID_WEBHOOK
        }
    })
    headers = {
        'Content-Type': 'application/json'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload)
    response_data = response.json()
    public_token = response_data.get('public_token')
    
    return public_token


def exchange_public_token_for_access_token(user_id, public_token):
    """Exchange public token for access token and store the relationship with user"""
    url = f"https://{PLAID_ENV}.plaid.com/item/public_token/exchange"
    
    print(f"Exchanging public token: {public_token[:10]}...")
    
    payload = json.dumps({
        "client_id": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET_KEY,
        "public_token": public_token
    })
    headers = {
        'Content-Type': 'application/json'
    }

    print("Sending request to Plaid API...")
    response = requests.request("POST", url, headers=headers, data=payload)
    print(f"Response status code: {response.status_code}")
    print(f"Response body: {response.text}")
    
    response_data = response.json()
    
    # Check for errors in the response
    if 'error' in response_data:
        print(f"Error in Plaid API response: {response_data['error']}")
        raise Exception(f"Plaid API error: {response_data.get('error_message', 'Unknown error')}")

    access_token = response_data.get('access_token')
    item_id = response_data.get('item_id')
    
    if not access_token or not item_id:
        print("Error: Missing access_token or item_id in response")
        print(f"Response data: {response_data}")
        raise Exception("Missing access_token or item_id in Plaid API response")

    print(f"Successfully received access_token: {access_token[:10]}... and item_id: {item_id[:10]}...")

    # Store user-item relationship
    try:
        db_connection = dbf.connect_to_database()
        user_item_data = {
            'user_id': [user_id],
            'item_id': [item_id],
            'access_token': [access_token],
            'created_at': [pd.Timestamp.now()]
        }
        user_item_df = pd.DataFrame(user_item_data)
        
        print("Storing user-item relationship in database...")
        dbf.insert_dataframe_to_sql(user_item_df, 'Report_Plaid_User_Items', db_connection)
        print("Running stored procedure...")
        dbf.run_stored_procedure(db_connection, 'SP_Fill_Plaid_User_Items')
        print("Database operations completed successfully")
    except Exception as e:
        print(f"Database error: {e}")
        # Continue even if database operations fail
    
    return access_token


def retrieve_items(user_id=None):
    """Retrieve items, optionally filtered by user_id"""
    url = f"https://{PLAID_ENV}.plaid.com/item/get"
    
    # Get access tokens from database
    db_connection = dbf.connect_to_database()
    try:
        if user_id:
            query = f"SELECT access_token FROM Report_Plaid_User_Items WHERE user_id = '{user_id}'"
        else:
            query = "SELECT access_token FROM Report_Plaid_User_Items"
        
        access_tokens_df = pd.read_sql(query, db_connection)
        access_tokens = access_tokens_df['access_token'].tolist()
    except Exception as e:
        print(f"Database query error: {e}")
        return []
    
    all_items = []
    
    for access_token in access_tokens:
        payload = json.dumps({
            "client_id": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET_KEY,
            "access_token": access_token
        })
        headers = {
            'Content-Type': 'application/json'
        }

        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            response_data = response.json()
            
            # Check if 'item' key exists in response_data
            if 'item' not in response_data:
                print(f"Error: 'item' key not found in response. Response: {response_data}")
                continue

            # Extract item details into a DataFrame
            item_data = {
                'consent_expiration_time': [response_data['item'].get('consent_expiration_time')],
                'created_at': [response_data['item'].get('created_at')],
                'error': [response_data['item'].get('error')],
                'institution_id': [response_data['item'].get('institution_id')],
                'institution_name': [response_data['item'].get('institution_name')],
                'item_id': [response_data['item'].get('item_id')],
                'update_type': [response_data['item'].get('update_type')],
                'webhook': [response_data['item'].get('webhook')],
                'access_token': [access_token]
            }
            item_df = pd.DataFrame(item_data)

            # Create a DataFrame for each available product with item_id reference
            if 'available_products' in response_data['item']:
                available_products_data = {
                    'item_id': [response_data['item']['item_id']] * len(response_data['item']['available_products']),
                    'available_product': response_data['item']['available_products']
                }
                available_products_df = pd.DataFrame(available_products_data)
                print(available_products_df)

            # Create a DataFrame for each billed product with item_id reference
            if 'billed_products' in response_data['item']:
                billed_products_data = {
                    'item_id': [response_data['item']['item_id']] * len(response_data['item']['billed_products']),
                    'billed_product': response_data['item']['billed_products']
                }
                billed_products_df = pd.DataFrame(billed_products_data)
                print(billed_products_df)

            # Extract request details into a DataFrame
            if 'request_id' in response_data:
                request_data = {
                    'request_id': [response_data['request_id']]
                }
                request_df = pd.DataFrame(request_data)
                print(request_df)

            # Extract status details into a DataFrame
            if 'status' in response_data and 'transactions' in response_data['status']:
                status_data = {
                    'last_webhook': [response_data['status'].get('last_webhook')],
                    'last_failed_update': [response_data['status']['transactions'].get('last_failed_update')],
                    'last_successful_update': [response_data['status']['transactions'].get('last_successful_update')]
                }
                status_df = pd.DataFrame(status_data)
                print(status_df)

            print(item_df)
            try:
                dbf.insert_dataframe_to_sql(item_df, 'Report_Plaid_items', db_connection)
                dbf.run_stored_procedure(db_connection, 'SP_Fill_Plaid_items')
            except Exception as e:
                print(f"Database error when inserting item data: {e}")
                
        except Exception as e:
            print(f"Error retrieving item for access token {access_token}: {e}")


def get_access_token_for_user(user_id):
    """Get access token for a specific user"""
    try:
        db_connection = dbf.connect_to_database()
        query = f"SELECT access_token FROM Report_Plaid_User_Items WHERE user_id = '{user_id}'"
        access_tokens_df = pd.read_sql(query, db_connection)
        
        # Get access token as string with error handling
        access_token_str = access_tokens_df['access_token'].iloc[0] if not access_tokens_df.empty else None
        return access_token_str
    except Exception as e:
        print(f"Error getting access token for user {user_id}: {e}")
        return None


# Only run this code if the file is executed directly
if __name__ == "__main__":
    user_id = "user123"
    public_token = createItem_public_token()
    access_token = exchange_public_token_for_access_token(user_id, public_token)

    # Get access token for the user
    user_access_token = get_access_token_for_user(user_id)

    # Retrieve items for a specific user
    retrieve_items(user_id)

    # Retrieve all items
    retrieve_items()