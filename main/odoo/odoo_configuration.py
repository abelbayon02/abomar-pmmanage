from dotenv import load_dotenv
import xmlrpc.client
import re
import os

# Load environment variables
load_dotenv()

# Define the server details
url = os.getenv('ODOO_URL')
db = os.getenv('ODOO_DB')
username = os.getenv('ODOO_USERNAME')
password = os.getenv('ODOO_PASSWORD')
access_token = os.getenv('ACCESS_TOKEN')

def get_server_proxy(url, endpoint):
    url = url.rstrip('/')
    try:
        return xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/{endpoint}')
    except xmlrpc.client.ProtocolError as e:
        if e.code == 303:
            new_url = e.headers.get('Location')
            print(f"Redirected to: {new_url}")
            return xmlrpc.client.ServerProxy(f'{new_url}/xmlrpc/2/{endpoint}')
        else:
            raise

def fetch_data(server_proxy, model, method, domain=None, fields=None, groupby=None, lazy=True, limit=None, offset=0, values=None, ids=None):
    try:
        print(f"Method: {method}, Domain: {domain}, IDs: {ids}, Values: {values}")
        if method == 'search_read':
            if domain is None:
                domain = []
            if not isinstance(domain, list):
                raise ValueError("Domain must be a list.")
            if fields is not None and not isinstance(fields, list):
                raise ValueError("Fields must be a list if provided.")

            params = [domain]
            kwargs = {}

            if fields:
                kwargs['fields'] = fields
            if limit is not None:
                kwargs['limit'] = limit
            if offset > 0:
                kwargs['offset'] = offset

            return server_proxy.execute_kw(db, uid, password, model, method, params, kwargs)
        elif method == 'search':
            if not isinstance(domain, list):
                raise ValueError("Domain must be a list.")
            params = [domain]
            if limit is not None or offset > 0:
                params.append({'limit': limit, 'offset': offset})
            return server_proxy.execute_kw(db, uid, password, model, method, params)
        elif method == 'read':
            if not isinstance(domain, list) or not isinstance(fields, list):
                raise ValueError("Domain and fields must be lists.")
            return server_proxy.execute_kw(db, uid, password, model, method, [domain], {'fields': fields})
        elif method == 'search_count':
            if not isinstance(domain, list):
                raise ValueError("Domain must be a list.")
            return server_proxy.execute_kw(db, uid, password, model, method, [domain])
        elif method == 'read_group':
            if not isinstance(domain, list) or not isinstance(fields, list) or not isinstance(groupby, list):
                raise ValueError("Domain, fields, and groupby must be lists.")
            return server_proxy.execute_kw(db, uid, password, model, method, [domain], {
                'fields': fields, 'groupby': groupby, 'lazy': lazy
            })
        elif method == 'create':
            if values is None:
                raise ValueError("Values must be provided for the create method.")
            if not isinstance(values, list):
                raise ValueError("Values must be a list of dictionaries.")
            params = [values]  # Wrap in a list
            return server_proxy.execute_kw(db, uid, password, model, method, [values])
        elif method == 'write':
            if ids is None or not isinstance(ids, list):
                raise ValueError("IDs must be provided as a list for the write method.")
            if values is None or not isinstance(values, dict):
                raise ValueError("Values must be provided as a dictionary for the write method.")
            params = [ids, values]
            return server_proxy.execute_kw(db, uid, password, model, method, [ids, values])
        elif method == 'unlink':
            if ids is None or not isinstance(ids, list):
                raise ValueError("IDs must be provided as a list for the unlink method.")
            return server_proxy.execute_kw(db, uid, password, model, method, [ids])
        elif method in ['button_confirm', 'button_cancel', 'button_draft']:  # Add any other action methods here
            if ids is None or not isinstance(ids, list):
                raise ValueError("IDs must be provided as a list for the action methods.")
            return server_proxy.execute_kw(db, uid, password, model, method, [ids])
        
        else:
            raise ValueError(f"Unsupported method: {method}")
    except xmlrpc.client.Fault as e:
        print(f"Error fetching data from {model}: {e}")
        return []



# Connect to the common endpoint
common = get_server_proxy(url, 'common')

try:
    version = common.version()
except xmlrpc.client.Fault as e:
    exit(1)
except xmlrpc.client.ProtocolError as e:
    exit(1)

# Authenticate
try:
    uid = common.authenticate(db, username, password, {})
    if not uid:
        exit(1)
except xmlrpc.client.Fault as e:
    exit(1)
except xmlrpc.client.ProtocolError as e:
    exit(1)

models = get_server_proxy(url, 'object')