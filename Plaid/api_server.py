from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import sys
import os
import traceback

# Add the parent directory to the path so we can import your existing modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your existing Python modules
from Items import exchange_public_token_for_access_token, get_access_token_for_user
from Transactions import TransactionsSync
from Configuration import PLAID_CLIENT_ID, PLAID_SECRET_KEY, PLAID_ENV, PLAID_WEBHOOK
import requests
import json

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Serve the index.html file from the public directory
@app.route('/')
def serve_index():
    try:
        return send_from_directory('public', 'index.html')
    except Exception as e:
        print(f"Error serving index.html: {e}")
        return jsonify({"error": "Could not serve index.html"}), 500

# Serve any static files from the public directory
@app.route('/<path:path>')
def serve_static(path):
    try:
        return send_from_directory('public', path)
    except Exception as e:
        print(f"Error serving static file {path}: {e}")
        return jsonify({"error": f"Could not serve {path}"}), 500

@app.route('/api/create-link-token', methods=['GET'])
def create_link_token():
    """
    Create a link token for Plaid Link
    This endpoint will be called by your frontend
    """
    try:
        url = f"https://{PLAID_ENV}.plaid.com/link/token/create"
        
        # You should get the user ID from your authentication system
        user_id = 'user123'  # Replace with actual user ID
        
        # Print environment info for debugging
        print(f"Creating link token with environment: {PLAID_ENV}")
        print(f"Client ID: {PLAID_CLIENT_ID[:5]}... (truncated)")
        print(f"Secret Key: {PLAID_SECRET_KEY[:5]}... (truncated)")
        
        payload = json.dumps({
            "client_id": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET_KEY,
            "client_name": "Personal Finance App",
            "user": {
                "client_user_id": user_id
            },
            "products": ["transactions"],
            "country_codes": ["US"],
            "language": "en",
            "webhook": PLAID_WEBHOOK,
            # Add Data Transparency Messaging configuration
            "update_type": "manual",
            "link_customization_name": "default",
            "data_transparency": {
                "use_cases": ["PERSONAL_FINANCE"],
                "opt_out_enabled": False,
                "show_use_cases": True,
                "show_detailed_scopes": True,
                "show_third_party_consent": True
            }
        })
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        print("Sending request to Plaid API...")
        response = requests.post(url, headers=headers, data=payload)
        print(f"Response status code: {response.status_code}")
        
        # Print full response for debugging
        print(f"Full response: {response.text}")
        
        response_data = response.json()
        
        if 'error' in response_data:
            print(f"Plaid API error: {response_data['error']}")
            return jsonify({"error": response_data['error']}), 400
            
        print("Link token created successfully")
        return jsonify(response_data)
    except Exception as e:
        print(f"Error creating link token: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/exchange-public-token', methods=['POST'])
def exchange_token():
    """
    Exchange a public token for an access token
    This endpoint will be called by your frontend after successful Plaid Link flow
    """
    try:
        print("Received request to exchange public token")
        data = request.json
        print(f"Request data: {data}")
        
        public_token = data.get('public_token')
        
        if not public_token:
            print("Error: Missing public token in request")
            return jsonify({"error": "Missing public token"}), 400
        
        print(f"Public token received: {public_token[:10]}...")
        
        # Use your existing function to exchange the token
        user_id = 'user123'  # Replace with actual user ID
        print(f"Exchanging token for user: {user_id}")
        
        access_token = exchange_public_token_for_access_token(user_id, public_token)
        print(f"Access token received: {access_token[:10] if access_token else 'None'}...")
        
        if not access_token:
            print("Error: Failed to get access token")
            return jsonify({"error": "Failed to get access token"}), 500
        
        print("Successfully exchanged public token for access token")
        return jsonify({"success": True, "message": "Successfully linked account"})
    except Exception as e:
        print(f"Error exchanging public token: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/transactions', methods=['GET'])
def get_transactions():
    """
    Get transactions for the current user
    """
    try:
        # Call your existing function to sync transactions
        user_id = 'user123'  # Replace with actual user ID
        TransactionsSync(user_id)
        return jsonify({"success": True, "message": "Transactions synced successfully"})
    except Exception as e:
        print(f"Error syncing transactions: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/test', methods=['GET'])
def test():
    """
    Simple test endpoint to verify the server is working
    """
    return jsonify({"message": "API server is working!"})

@app.route('/api/create-sandbox-public-token', methods=['GET'])
def create_sandbox_public_token():
    """
    Create a sandbox public token directly
    This is a workaround for institution registration issues
    """
    try:
        url = f"https://{PLAID_ENV}.plaid.com/sandbox/public_token/create"
        
        payload = json.dumps({
            "client_id": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET_KEY,
            "institution_id": "ins_109508", # Chase Bank in sandbox
            "initial_products": ["transactions"],
            # Add Data Transparency Messaging configuration
            "options": {
                "webhook": PLAID_WEBHOOK
            }
        })
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        print("Creating sandbox public token directly...")
        response = requests.post(url, headers=headers, data=payload)
        print(f"Response status code: {response.status_code}")
        print(f"Response body: {response.text}")
        
        response_data = response.json()
        
        if 'error' in response_data:
            print(f"Plaid API error: {response_data['error']}")
            return jsonify({"error": response_data['error']}), 400
        
        public_token = response_data.get('public_token')
        
        if not public_token:
            print("Error: Missing public_token in response")
            return jsonify({"error": "Missing public_token in response"}), 500
        
        print(f"Successfully created sandbox public token: {public_token[:10]}...")
        
        # Now exchange this public token for an access token
        user_id = 'user123'  # Replace with actual user ID
        access_token = exchange_public_token_for_access_token(user_id, public_token)
        
        return jsonify({
            "success": True, 
            "message": "Successfully created and exchanged sandbox public token",
            "public_token": public_token[:10] + "..." # Only return part of the token for security
        })
    except Exception as e:
        print(f"Error creating sandbox public token: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("Starting Flask server...")
    print(f"Plaid environment: {PLAID_ENV}")
    print(f"Client ID present: {'Yes' if PLAID_CLIENT_ID else 'No'}")
    print(f"Secret key present: {'Yes' if PLAID_SECRET_KEY else 'No'}")
    app.run(port=5000, debug=True) 