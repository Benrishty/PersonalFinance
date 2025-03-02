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
    
    payload = json.dumps({
        "client_id": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET_KEY,
        "public_token": public_token
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response_data = response.json()

    access_token = response_data.get('access_token')
    item_id = response_data.get('item_id')

    # Store user-item relationship
    db_connection = dbf.connect_to_database()
    user_item_data = {
        'user_id': [user_id],
        'item_id': [item_id],
        'access_token': [access_token],
        'created_at': [pd.Timestamp.now()]
    }
    user_item_df = pd.DataFrame(user_item_data)
    dbf.insert_dataframe_to_sql(user_item_df, 'Plaid_User_Items', db_connection)

    return access_token



def retrieve_items(user_id=None):
    """Retrieve items, optionally filtered by user_id"""
    url = f"https://{PLAID_ENV}.plaid.com/item/get"
    
    # Get access tokens from database
    db_connection = dbf.connect_to_database()
    if user_id:
        query = f"SELECT access_token FROM Plaid_User_Items WHERE user_id = '{user_id}'"
    else:
        query = "SELECT access_token FROM Plaid_User_Items"
    
    access_tokens_df = pd.read_sql(query, db_connection)
    access_tokens = access_tokens_df['access_token'].tolist()
    
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

        response = requests.request("POST", url, headers=headers, data=payload)
        response_data = response.json()

        # # Flatten the nested JSON structure
        # flattened_data = {
        #     # Item details
        #     'available_products': ','.join(response_data['item']['available_products']),
        #     'billed_products': ','.join(response_data['item']['billed_products']),
        #     'consent_expiration_time': response_data['item']['consent_expiration_time'],
        #     'created_at': response_data['item']['created_at'],
        #     'error': response_data['item']['error'],
        #     'institution_id': response_data['item']['institution_id'],
        #     'institution_name': response_data['item']['institution_name'],
        #     'item_id': response_data['item']['item_id'],
        #     'products': ','.join(response_data['item']['products']),
        #     'update_type': response_data['item']['update_type'],
        #     'webhook': response_data['item']['webhook'],
        #     # Request details
        #     'request_id': response_data['request_id'],
        #     # Status details
        #     'last_webhook': response_data['status']['last_webhook'],
        #     'last_failed_update': response_data['status']['transactions']['last_failed_update'],
        #     'last_successful_update': response_data['status']['transactions']['last_successful_update']
        # }

        # all_items.append(flattened_data)

        # Extract item details into a DataFrame
        item_data = {
            'consent_expiration_time': [response_data['item']['consent_expiration_time']],
            'created_at': [response_data['item']['created_at']],
            'error': [response_data['item']['error']],
            'institution_id': [response_data['item']['institution_id']],
            'institution_name': [response_data['item']['institution_name']],
            'item_id': [response_data['item']['item_id']],
            'update_type': [response_data['item']['update_type']],
            'webhook': [response_data['item']['webhook']],
            'access_token': [access_token]
        }
        item_df = pd.DataFrame(item_data)

        # Create a DataFrame for each available product with item_id reference
        available_products_data = {
            'item_id': [response_data['item']['item_id']] * len(response_data['item']['available_products']),
            'available_product': response_data['item']['available_products']
        }
        available_products_df = pd.DataFrame(available_products_data)

        # Create a DataFrame for each billed product with item_id reference
        billed_products_data = {
            'item_id': [response_data['item']['item_id']] * len(response_data['item']['billed_products']),
            'billed_product': response_data['item']['billed_products']
        }
        billed_products_df = pd.DataFrame(billed_products_data)

        # Extract request details into a DataFrame
        request_data = {
            'request_id': [response_data['request_id']]
        }
        request_df = pd.DataFrame(request_data)

        # Extract status details into a DataFrame
        status_data = {
            'last_webhook': [response_data['status']['last_webhook']],
            'last_failed_update': [response_data['status']['transactions']['last_failed_update']],
            'last_successful_update': [response_data['status']['transactions']['last_successful_update']]
        }
        status_df = pd.DataFrame(status_data)

        print(item_df)
        dbf.insert_dataframe_to_sql(item_df, 'Temp_Plaid_item_df', db_connection)

        print(available_products_df)
        # dbf.insert_dataframe_to_sql(available_products_df, 'Temp_Plaid_available_products_df', db_connection)

        print(billed_products_df)
        # dbf.insert_dataframe_to_sql(billed_products_df, 'Temp_Plaid_billed_products_df', db_connection)

        print(request_df)
        # dbf.insert_dataframe_to_sql(request_df, 'Temp_Plaid_request_df', db_connection)
        
        print(status_df)
        # dbf.insert_dataframe_to_sql(status_df, 'Temp_Plaid_status_df', db_connection)


user_id = "user123"
public_token = createItem_public_token()
access_token = exchange_public_token_for_access_token(user_id, public_token)


# Retrieve items for a specific user
retrieve_items(user_id)

# Retrieve all items
retrieve_items()