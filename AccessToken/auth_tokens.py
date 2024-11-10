from dotenv import load_dotenv
import requests
import os
from requests.auth import HTTPBasicAuth

load_dotenv()

# OAuth 2.0 credentials
# client_id = '0oaythv1mgKlsvK981t7'
# client_secret = 'fle58jHr7yqnGqzL3_IpAY5kEPhEotqX7cHGFA9xxvUvxpRJ0TMgHFJE2f7E31b6'
# token_url = 'https://sso-cert.johndeere.com/oauth2/aus97etlxsNTFzHT11t7/v1/token'
# grant_type = 'client_credentials'

client_id = '0oaytimtyvhNqD3Hs1t7'
client_secret = '1w_TSJgC5YCtR7wK8uCeaz8BSiR5ysR3AIS6PYREY1e76h4u_uWyNSvQNeGRbsXG'
token_url = 'https://sso.johndeere.com/oauth2/aus9k0fb8kUjG8S5Z1t7/v1/token'
grant_type = 'client_credentials'

# client_id = os.getenv('CLIENT_ID')
# client_secret = os.getenv('CLIENT_SECRET')
# token_url = os.getenv('TOKEN_URL')
# grant_type = os.getenv('GRANT_TYPE')

# print(f"Ty: {os.getenv('GRANT_TYPE')}")



def get_access_token(scope):
    payload = {
        'grant_type': grant_type,
        'scope': scope
    }

    auth = HTTPBasicAuth(client_id, client_secret)

    try:
        response = requests.post(token_url, data=payload, auth=auth)

        if response.status_code == 200:
            token_info = response.json()
            access_token = token_info.get('access_token')
            return access_token
        else:
            print(f'Failed to obtain token: {response.status_code}')
            print(response.json())
            return None
    except requests.exceptions.RequestException as e:
        print(f'An error occurred: {e}')
        return None

def get_upload_token():
    upload_scope = 'dtf:dbs:file:read dtf:dbs:file:write'
    return get_access_token(upload_scope)

def get_download_token():
    download_scope = 'dtf:dbs:file:read'
    return get_access_token(download_scope)

if __name__ == "__main__":
    upload_token = get_upload_token()
    if upload_token:
        print(f'Upload Access Token: {upload_token}')

    download_token = get_download_token()
    if download_token:
        print(f'Download Access Token: {download_token}')