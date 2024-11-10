from dotenv import load_dotenv
from datetime import datetime
import sys
import os
import requests
import pytz
import json
import gzip
import shutil
import io

load_dotenv()
api_key = os.getenv('CLIENT_ID')

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'AccessToken'))

from auth_tokens import get_upload_token
from auth_tokens import get_download_token

download_token = get_download_token()

def get_downloadable_price_file(download_api_url, bearer_token):
    try:
        headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json'
        }

        response = requests.get(download_api_url, headers=headers)
        response.raise_for_status()

        data = response.json()

        return data
    except Exception as e:
        print(f"Failed to download the file: {e}")

def download_and_extract_files(download_token, files, download_dir):
    try:
        headers = {
            'Authorization': f'Bearer {download_token}',
            'Content-Type': 'application/json'
        }

        compressed_dir = os.path.join(base_dir, 'Compressed_Files')
        decompressed_dir = os.path.join(base_dir, 'Decompressed_Files')

        os.makedirs(compressed_dir, exist_ok=True)
        os.makedirs(decompressed_dir, exist_ok=True)
        
        for file_info in files['files']:
            file_name = file_info['name']
            if 'PARTINFO' in file_name and file_name.endswith('.dtz'):
                download_link = next(link['href'] for link in file_info['links'] if link['rel'] == 'download')
                response = requests.get(download_link, headers=headers)
                
                if response.status_code == 200:
                    compressed_file_path = os.path.join(compressed_dir, file_name)
                    
                    # Save the downloaded .dtz file in Compressed_Files folder
                    with open(compressed_file_path, 'wb') as f:
                        f.write(response.content)
                    print(f'Saved: {file_name} to {compressed_file_path}')
                    
                    # Decompress the .dtz file as if it were a gzip file
                    decompressed_file_path = os.path.join(decompressed_dir, file_name.replace('.dtz', ''))

                    with gzip.open(compressed_file_path, 'rb') as f_in:
                        with open(decompressed_file_path, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    print(f'Decompressed and saved: {file_name} to {decompressed_file_path}')
                else:
                    print(f'Failed to download {file_name}. Status code: {response.status_code}')
    except Exception as e:
        print(f"Failed to download or extract the file: {e}")


if __name__ == '__main__':
    download_api_url = 'https://dtfapi.deere.com/dbs/dealer/7A1758/files/'
    bearer_token = download_token
    base_dir = '../Files'

    result = get_downloadable_price_file(download_api_url, download_token)
    #print(result)
    if result and 'files' in result and result['files']:
        download_and_extract_files(download_token, result, base_dir)
    else:
        print("No downloadable DPMORD files found.")
    # if bearer_token: 
    #     get_downloadable_price_file(download_api_url, bearer_token) 