import requests
import json
import pandas as pd
import DatabaseFunctions as dbf
from Configuration import PLAID_CLIENT_ID, PLAID_SECRET_KEY, PLAID_ENV, PLAID_WEBHOOK



def createItem_public_token():
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
    # Get the datatype of the response
    
    response_data = response.json()
    public_token = response_data.get('public_token')

    
    # # Create a DataFrame from the response data
    # df = pd.DataFrame([response_data])  # Wrap in a list to create a DataFrame
    
    return public_token
    # # Insert DataFrame to Database
    # db_connection = dbf.connect_to_database()
    # dbf.insert_dataframe_to_sql(df, 'test_public_token', db_connection)


def exchange_public_token_for_access_token():
    url = f"https://{PLAID_ENV}.plaid.com/item/public_token/exchange"
    
    public_token = createItem_public_token()
    
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

    return access_token



def retrieve_items():
    """Retrieve multiple items and return them in a DataFrame"""
    url = f"https://{PLAID_ENV}.plaid.com/item/get"
    
    # You might get these from your database or another source
    access_tokens = [
        exchange_public_token_for_access_token(),
        # Add more access tokens as needed
    ]
    
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

        # Flatten the nested JSON structure
        flattened_data = {
            # Item details
            'available_products': ','.join(response_data['item']['available_products']),
            'billed_products': ','.join(response_data['item']['billed_products']),
            'consent_expiration_time': response_data['item']['consent_expiration_time'],
            'created_at': response_data['item']['created_at'],
            'error': response_data['item']['error'],
            'institution_id': response_data['item']['institution_id'],
            'institution_name': response_data['item']['institution_name'],
            'item_id': response_data['item']['item_id'],
            'products': ','.join(response_data['item']['products']),
            'update_type': response_data['item']['update_type'],
            'webhook': response_data['item']['webhook'],
            # Request details
            'request_id': response_data['request_id'],
            # Status details
            'last_webhook': response_data['status']['last_webhook'],
            'last_failed_update': response_data['status']['transactions']['last_failed_update'],
            'last_successful_update': response_data['status']['transactions']['last_successful_update']
        }
        
        all_items.append(flattened_data)

    # Create DataFrame from all flattened items
    df = pd.DataFrame(all_items)
    
    return df


def retrieve_items_from_db():
    """Retrieve items using access tokens stored in database"""
    db_connection = dbf.connect_to_database()
    
    # Assuming you have a table with access tokens
    query = "SELECT access_token FROM plaid_tokens"
    access_tokens_df = pd.read_sql(query, db_connection)
    
    all_items = []
    
    for access_token in access_tokens_df['access_token']:
        # ... same code as above to get and flatten item data ...
        all_items.append(flattened_data)
    
    df = pd.DataFrame(all_items)
    
    # Optionally save results back to database
    dbf.insert_dataframe_to_sql(df, 'plaid_items', db_connection)
    
    return df



retrieve_items()