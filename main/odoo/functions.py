#from odoo_configuration_pmmanage import get_server_proxy, fetch_data, db, username, password, uid, url, models, common

from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import defaultdict
import re
import sys
import time
import random
import xmlrpc.client
import shutil
import os
import json
# import openpyxl
import base64
from concurrent.futures import ThreadPoolExecutor
import xlsxwriter
from io import BytesIO
import base64
# import csv
from itertools import islice
import concurrent.futures
import traceback
import logging

module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if module_path not in sys.path:
    sys.path.append(module_path)


from odoo_config.odoo_configuration_pmmanage import fetch_data, db, password, uid, models
from email_sending import send_email

output_json_path = "/var/www/abomar-pmm-api/abomar-pmm/main/odoo/matched_products.json"
log_filename = "/var/www/abomar-pmm-api/abomar-pmm/main/odoo/PRICEFILE.log" 

logging.basicConfig(
    filename=log_filename,
    filemode="a",  # Append mode: new log entries are added to the existing file
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def GET_CATEGORY():
    category_name = 'Parts / AG / Tractor / John Deere'
    category = fetch_data(models, 'product.category', 'search_read',
                          domain=[('complete_name', 'ilike', category_name)],
                          fields=['id'],
                        )
                         
    
    
    return category[0]['id'] if category else None

def GET_PARTNERS():
    try:
        # Fetch partners using the domain filter
        vendors = fetch_data(
            models,
            'res.partner',
            method='search_read',
            domain=[('name', 'ilike', 'John Deere')],
            fields=['id'] 
        )
        
        return vendors[0]['id'] if vendors else None
    except Exception as e:
        print(f"Error fetching vendor: {e}")
        return None
    
CATEGORY_ID = GET_CATEGORY()
if CATEGORY_ID is None:
    raise ValueError("Category 'Parts / AG / Tractor / John Deere' not found.")

PARTNER_ID = GET_PARTNERS()
if PARTNER_ID is None:
    raise ValueError("Partner 'John Deere' not found.")

def ADD_NEW_PRODUCTS(missing_product_batch, retries=5):
    """
    Insert new products and their supplier info in batches.
    """
    attempt = 0
    added_product_tmpl_ids = {}
    current_batch = []
    supplierinfo_batch = []
    batch_size = 20000

    while attempt <= retries:
        try:
            print(f"Product Doesn't Exist")
            # Fetch category ID
            category_id = CATEGORY_ID
            if not category_id:
                print("Category not found!")
                return None

            for product in missing_product_batch:
                # Define the product data
                current_batch.append({
                    'default_code': product['product_code'],
                    'code': product['product_code'],  # Assuming 'code' is the same as 'default_code'
                    'display_name': product['product_name'],
                    'name': product['product_name'],
                    'categ_id': CATEGORY_ID
                })

                # Process the batch when batch_size is reached
                if len(current_batch) >= batch_size:
                    added_tmpl_ids = _process_product_insert_batch(current_batch, retries)
                    if added_tmpl_ids:
                        # Map product code to template IDs for creating supplier info
                        for idx, tmpl_id in enumerate(added_tmpl_ids):
                            added_product_tmpl_ids[missing_product_batch[idx]['product_code']] = tmpl_id

                    # Prepare supplierinfo batch
                    supplierinfo_batch.extend(_prepare_supplierinfo_batch(missing_product_batch, added_product_tmpl_ids))
                    current_batch = []  # Clear the batch for the next set

            # Process any remaining products in the batch
            if current_batch:
                added_tmpl_ids = _process_product_insert_batch(current_batch, retries)
                if added_tmpl_ids:
                    for idx, tmpl_id in enumerate(added_tmpl_ids):
                        added_product_tmpl_ids[missing_product_batch[idx]['product_code']] = tmpl_id

                # Prepare supplierinfo batch
                supplierinfo_batch.extend(_prepare_supplierinfo_batch(missing_product_batch, added_product_tmpl_ids))

            # Insert supplier information after processing all batches
            ADD_SUPPLIERINFO_BATCH(supplierinfo_batch, retries)

            print("All products and supplier information added successfully.")
            return list(added_product_tmpl_ids.values())

        except Exception as e:
            print(f"Error adding products and supplier info in batch: {e}")
            return []
        
def _prepare_supplierinfo_batch(missing_product_batch, added_product_tmpl_ids):
    """
    Prepare batch data for product.supplierinfo insertion.

    Args:
        missing_product_batch (list): The original missing product data batch.
        added_product_tmpl_ids (dict): A mapping of product_code to product_tmpl_id.

    Returns:
        list: A list of dictionaries to insert into product.supplierinfo.
    """
    routes = GET_ROUTE_IDS()
    # Create a mapping of product_code to route IDs if necessary
    route_ids = [route['id'] for route in routes] if routes else []
    supplierinfo_batch = []
    for product in missing_product_batch:
        product_code = product['product_code']
        if product_code in added_product_tmpl_ids:
            vals = {
                'product_name': product.get('product_name', ''),
                'product_code': product.get('product_code', ''),
                'x_studio_jd_list_price': product.get('x_studio_jd_list_price', 0.0),
                'product_tmpl_id': added_product_tmpl_ids[product_code],
                'price': product.get('price', 0.0),
                'min_qty': product.get('min_qty', 1),
                'currency_id': product.get('currency_id', 1),
                'partner_id': product.get('partner_id', 0),
                'date_start': product.get('start_date', '')
            }

            if route_ids:
                vals['route_ids'] = [(6, 0, route_ids)]

            # Print the dictionary for each product to check its structure
            print(f"Record being added: {vals}")

            # Append the dictionary to supplierinfo_batch
            supplierinfo_batch.append(vals)
            
    return supplierinfo_batch

def ADD_SUPPLIERINFO_BATCH(supplierinfo_batch, retries=5, batch_size=20000):
    """
    Insert supplier information in batches into product.supplierinfo.
    """
    attempt = 0
    while attempt <= retries:
        try:
            for i in range(0, len(supplierinfo_batch), batch_size):
                current_batch = supplierinfo_batch[i:i + batch_size]
                fetch_data(models, 'product.supplierinfo', 'create', values=current_batch)

            print("All supplier information added successfully.")
            return True

        except xmlrpc.client.ProtocolError as e:
            if e.errcode == 429:  # Rate limit exceeded
                attempt += 1
                wait_time = 2 ** attempt + random.uniform(0, 1)
                print(f"Rate limit reached. Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            print(f"Error inserting supplier information: {e}")
            raise


def _process_product_insert_batch(batch, retries):
    """
    Helper function to process a batch of products.

    Args:
        batch (list): A list of product data dictionaries.
        retries (int): Number of retries for rate-limit errors.

    Returns:
        list: A list of added product template IDs.
    """
    attempt = 0
    while attempt <= retries:
        try:
            # Debug: Print the batch before inserting
            print(f"Inserting batch: {batch}")
            
            # Create products in batch
            product_ids = fetch_data(models, 'product.product', 'create', values=batch)  # Pass batch directly, not wrapped in another list
            
            # Debug: Print created product IDs
            print(f"Created product IDs: {product_ids}")
            
            if not product_ids:
                print("Error: No product IDs returned after creation.")
                return []

            # Fetch product_tmpl_id for each newly created product
            new_products = fetch_data(
                models,
                'product.product',
                'search_read',
                domain=[('id', 'in', product_ids)],
                fields=['product_tmpl_id'],
                limit=len(product_ids)
            )

            # Debug: Print the fetched products
            print(f"Fetched products for template IDs: {new_products}")
            
            # Extract and return product template IDs
            if new_products:
                return [
                    product['product_tmpl_id'][0]  # Assuming product_tmpl_id is a list
                    for product in new_products
                    if 'product_tmpl_id' in product and isinstance(product['product_tmpl_id'], list)
                ]
            return []  # Return empty list if no products or template IDs found

        except xmlrpc.client.ProtocolError as e:
            if e.errcode == 429:  # Rate limit exceeded
                attempt += 1
                wait_time = 2 ** attempt + random.uniform(0, 1)
                print(f"Rate limit reached. Retrying in {wait_time:.2f} seconds...")
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            print(f"Error processing batch: {e}")
            return []  # Return empty list on failure

    return []  # Return empty list if retries are exhausted


def fetch_batch(offset, batch_size):
    try:
        # Fetch product IDs
        product_ids = fetch_data(
            models,
            'product.product',
            'search',
            domain=[('categ_id.name', '=', 'John Deere')],
            limit=batch_size,
            offset=offset
        )
        if not product_ids:
            return []

        # Read required fields for the IDs
        products = fetch_data(
            models,
            'product.product',
            'read',
            ids=product_ids,
            fields=['default_code', 'product_tmpl_id']
        )
        return products
    except Exception as e:
        print(f"Error fetching batch at Offset={offset}: {e}")
        return []
    

def GET_PRODUCT_TMPL_ID(retries=5, batch_size=100000, max_workers=4):
    result_dict = defaultdict(set)
    offset = 0
    batch_count = 0
    start_time = time.time()

    # Use ThreadPoolExecutor for parallel data fetching
    print(f"Storing All Products in a dictionary")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        while True:
            # Submit batch fetching tasks
            futures.append(executor.submit(fetch_batch, offset, batch_size))
            offset += batch_size
            batch_count += 1

            # Stop submitting when no more data is expected
            if len(futures) > 0 and not futures[-1].result():
                break

        # Process results
        for i, future in enumerate(futures, start=1):
            try:
                products = future.result()
                for product in products:
                    product_tmpl_id = product.get('product_tmpl_id')
                    product_code_value = product.get('default_code')

                    if product_code_value:
                        if isinstance(product_tmpl_id, list):
                            product_tmpl_id = product_tmpl_id[0]
                        result_dict[product_code_value].add(product_tmpl_id)
            except Exception as e:
                print(f"Error processing batch {i}: {e}")

    end_time = time.time()
    return dict(result_dict)

PRODUCTTMPL_DICTIONARY = GET_PRODUCT_TMPL_ID()

def GET_ROUTE_IDS():
    """
    Fetch all available route IDs from the 'stock.route' model.

    Returns:
        list: A list of dictionaries containing route IDs, e.g., [{'id': 37}, {'id': 26}, ...].
              Returns an empty list if no routes are found.
    """
    try:
        # Fetch the routes
        routes = fetch_data(models, 'stock.route', 'search_read', domain=[], fields=['id'])
        if not routes:
            print("No routes found.")
        return routes
    except Exception as e:
        print(f"Error fetching route IDs: {e}")
        return []
                    
def GET_PRICELISTS():
    products = fetch_data(models, 'product.supplierinfo', 'search_read',
                          domain=[],
                          fields=[]
                        )   
    
    return products
def GET_CURRENCY(type,  retries=5):

    attempt = 0
    while attempt <= retries:
        try:
            currency = fetch_data(models, 'res.currency', 'search_read', domain=[('name', '=', 'USD')], fields=['id'])
    
            if currency:
                return currency[0]  # Return the first dictionary if currency is found
            else:
                return None
        except xmlrpc.client.ProtocolError as e:
            if e.errcode == 429:
                attempt += 1
                wait_time = 2 ** attempt + random.uniform(0, 1)
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            raise

    return None
   

def FORMAT_DATE(date, effective_date):
    try:
        # Convert from 'YYYYMMDD' to 'YYYY-MM-DD'
        if date:
            formatted_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
            return formatted_date
        elif effective_date:
            formatted_date = datetime.strptime(effective_date, "%Y%m%d").strftime("%Y-%m-%d")
            return formatted_date
        else:
            print("Both date and effective_date are missing.")
            return None  # Explicitly return None for invalid dates
    except ValueError as e:
        print(f"Invalid date format: {e}")
        return None  # Return None for any parsing errors

def PROCESS_BY_BATCH(data_batch, batch_type, batch_size=20000, retries=10):
    """
    Process a batch of data, retrying if rate limit is exceeded (HTTP 429).
    Handles both full and incremental batch types. 
    Deletes vendor price list entries as needed.
    """
    attempt = 0
    while attempt <= retries:
        try:
            # Handle deletion logic based on batch type
            if batch_type == 'FULL':
                # Delete all vendor pricelists if batch type is FULL
                print("ALL PRICELIST HAS BEEN TRUNCATED, INSERTING NEW...")

            elif batch_type == 'NET':
                # Delete specific vendor pricelists if batch type is INCREMENTAL
                print("Deleting specific vendor pricelists for INCREMENTAL batch...")
                product_ids = [data['product_code'] for data in data_batch]  # Collect product IDs from the batch
                # DELETE_SPECIFIC_VENDOR_PRICELISTS(product_ids, batch_size)

            # Call fetch_data to create records in Odoo
            price = fetch_data(
                models,
                'product.supplierinfo',
                'create',
                values=data_batch  # Corrected from [data_batch] to data_batch
            )


            print(f"Batch processed successfully.")
            return True  # Batch processed successfully
        except xmlrpc.client.ProtocolError as e:
            if e.errcode == 429:
                # Retry logic in case of rate limit error (HTTP 429)
                print(f"Rate limit exceeded. Retrying attempt {attempt}...")
                attempt += 1
                wait_time = 2 ** attempt + random.uniform(0, 1)
                print(f"Waiting for {wait_time:.2f} seconds before retrying.")
                time.sleep(wait_time)
            else:
                # Raise other exceptions if needed
                raise
        except Exception as e:
            print(f"Error processing batch: {e}")
            raise

    # If all retry attempts fail
    print("Failed to process batch after multiple retries.")
    return False


def PROCESS_DAT_PRICE_FILE(directory, file_folder, type, batch_size=20000, retry=5):
    """
    Process DAT price file in batches, retrying if rate limits are encountered.
    """
    batch_existing = []  # For existing products in PRODUCTTMPL_DICTIONARY
    batch_missing = []   # For products missing in PRODUCTTMPL_DICTIONARY
    specific_product_ids = []  # To store product IDs for specific deletion
    processed_count = 0
    try:
        if type == 'FULL':
            DELETE_ALL_VENDOR_PRICELISTS(batch_size=batch_size, file_folder=file_folder)  # Uncomment as needed
            print("Processing FULL batch, deleting all vendor pricelists.")
        elif type == 'NET':
            print("Gathering product IDs for specific vendor price list deletions...")

        print(f"FOLDER FOR BACKUP {file_folder}")

        for filename in os.listdir(directory):
            if filename.endswith(".DAT"):
                file_path = os.path.join(directory, filename)
                vendor_id = PARTNER_ID  # Assuming PARTNER_ID is defined
                # Set 'partner_id' field
                partner_id = vendor_id if vendor_id else 0
                with open(file_path, 'r', encoding='latin1') as dat_file:
                    header = next(dat_file)  # Skip header
                    effective_date = header[54:62]
                    quantity = 'E'
                    for line_number, line in enumerate(dat_file, start=2):
                        # Extract product data from the line
                        product_id = line[0:17].strip()
                        product_name = line[24:51].strip().lstrip('-')
                        start_date = line[208:216].strip()
                        if len(line) > 163:
                            quantity = line[163].strip() 
                        price = line[56:72].strip()

                        JD_list_price = line[72:87].strip()

                        # Process price and JD list price
                        price = price.lstrip('0')
                        JD_list_price = JD_list_price.lstrip('0')
                        price = '0' if price == '' else price
                        JD_list_price = '0' if JD_list_price == '' else JD_list_price

                        # Ensure prices are valid
                        if price.startswith('.'):
                            price = '0' + price
                        if JD_list_price.startswith('.'):
                            JD_list_price = '0' + JD_list_price

                        start_date_formatted = FORMAT_DATE(start_date, effective_date)  # Assuming FORMAT_DATE is defined
                        today = datetime.today().date()

                        # Only check the start date if type is NET
                        # client requested change , remove add product if doesnt exist in product template model - 2025/01/17
                        if type == 'NET' and start_date:
                            print("Preparing NET lines")
                            start_date_date = datetime.strptime(start_date_formatted, "%Y-%m-%d").date()
                            if start_date and start_date_date == today:
                                try:
                                    # Add product ID to the list for specific deletions
                                    specific_product_ids.append(product_id)
                                    
                                    # Safely retrieve product_tmpl_id from the dictionary
                                    product_tmpl_id = PRODUCTTMPL_DICTIONARY.get(product_id)
                                    
                                    # Ensure product_tmpl_id is not a set
                                    if isinstance(product_tmpl_id, set):
                                        product_tmpl_id = list(product_tmpl_id)[0] if product_tmpl_id else None
                                    
                                    # Validate and prepare data for the batch
                                    batch_existing.append({
                                        'partner_id': partner_id,
                                        'product_name': product_name,
                                        'product_code': product_id,
                                        'product_tmpl_id': product_tmpl_id if product_tmpl_id else False,
                                        'price': float(price),
                                        'x_studio_jd_list_price': float(JD_list_price),
                                        'min_qty': 1 if quantity == 'E' else 100 if quantity == 'C' else int(quantity) if quantity.isdigit() else 1,
                                        'currency_id': 1,
                                        'date_start': start_date_formatted
                                    })
                                except Exception as e:
                                    # Log the error for debugging
                                    print(f"Error processing product ID {product_id}: {str(e)}")
                                    # Optionally, log the product details for further investigation
                                    print(f"Product details: partner_id={partner_id}, product_name={product_name}, price={price}, quantity={quantity}")
                                    # Continue to the next product without terminating
                                    continue
                              
                                if len(batch_existing) >= batch_size:
                                    print(f"Batch size reached: {len(batch_existing)}. Sending to process...")
                                    if PROCESS_BY_BATCH(batch_existing, type):
                                        processed_count += len(batch_existing)
                                        batch_existing.clear()
                                        time.sleep(1)  # Add a small delay (e.g., 1 second)
                                    else:
                                        print("Failed to process the batch.")
                                        return False
                            else:
                                print("Start date not equal to today. Skipping...")
                               

                        elif type == 'FULL':  # If type is FULL, no need to check the start date
                            # if product_id in PRODUCTTMPL_DICTIONARY:  # Assuming PRODUCTTMPL_DICTIONARY is defined
                            # Fetch the associated product template ID
                            product_tmpl_id = PRODUCTTMPL_DICTIONARY.get(product_id)

                            if isinstance(product_tmpl_id, set):
                                product_tmpl_id = list(product_tmpl_id)[0] if product_tmpl_id else None

                            if start_date:  # Only append if start_date is not empty
                                batch_existing.append({
                                    'partner_id': partner_id,
                                    'product_name': product_name,
                                    'product_code': product_id,
                                    'product_tmpl_id': product_tmpl_id if product_tmpl_id else False,
                                    'price': float(price),
                                    'x_studio_jd_list_price': float(JD_list_price),
                                    'min_qty': 1 if quantity == 'E' else 100 if quantity == 'C' else int(quantity) if quantity.isdigit() else 1,
                                    'currency_id': 1,
                                    'date_start': start_date_formatted  # Only add if start_date is not empty
                                })

                            if len(batch_existing) >= batch_size:
                                print(f"Batch size reached: {len(batch_existing)}. Sending to process...")
                                if PROCESS_BY_BATCH(batch_existing, type):
                                    processed_count += len(batch_existing)
                                    batch_existing.clear()
                                    time.sleep(1)  # Add a small delay (e.g., 1 second)
                                else:
                                    print("Failed to process the batch.")
                                    return False
                                
        # Call DELETE_SPECIFIC_VENDOR_PRICELISTS if type is NET
        if type == 'NET' and specific_product_ids:
            print("Deleting specific vendor price lists...")
            DELETE_SPECIFIC_VENDOR_PRICELISTS(specific_product_ids,file_folder=file_folder)  # Uncomment as needed

        if batch_existing:
            if PROCESS_BY_BATCH(batch_existing, type):
                processed_count += len(batch_existing)
                print("Remaining batch processed successfully.")
            else:
                print("Failed to process remaining batch.")
                return "Failed to process remaining batch."
            
        # if batch_missing:
        #     print(f"Processing remaining missing products batch: {len(batch_missing)} items.")
        #     if not ADD_NEW_PRODUCTS(batch_missing):
        #         print("Failed to process remaining missing products batch.")
        #         return False

        print("All products loaded successfully.")
        if type == 'FULL':
            os.remove(file_path)

        subject = 'PRICE FILE STATUS'
        fileType = 'FULL' if type == 'FULL' else 'NET'

        body = (
            f"The {fileType} price file was successfully loaded on {datetime.today().strftime('%Y-%m-%d')}.\n\n"
            f"A total of {processed_count} record(s) have been updated."
        )

        __log_value = f"The {fileType} price file was successfully loaded on {datetime.today().strftime('%Y-%m-%d')}. A total of {processed_count} record(s) have been updated."


        send_email(body, subject)
        return __log_value  # Success if all batches are processed

    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"Error processing DAT price file: {e}")
        return error_trace  # Return False if an error occurs
    
def DELETE_SPECIFIC_VENDOR_PRICELISTS(product_codes, batch_size=20000, file_folder=None, max_rows_per_file=200000):
    """
    Deletes specific vendor price list entries based on product codes in batches and writes them concurrently to Excel files.
    """
    try:
        current_date = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        
        # Parent folder: Spreadsheets
        # parent_folder_path = "Spreadsheets"
        parent_folder_path = "/var/www/abomar-pmm-api/abomar-pmm/main/odoo/Spreadsheets"
        os.makedirs(parent_folder_path, exist_ok=True)  # Ensure the parent folder exists
        
        # Subfolder for the current backup: NET_{current_date}
        backup_folder_path = os.path.join(parent_folder_path, f"NET_PRICEFILE_BACKUP_{current_date}")
        os.makedirs(backup_folder_path, exist_ok=True)  # Create the subfolder for the backup
        
        print(f"Fetching specific vendor price list IDs...: {product_codes}")
        vendor_price_generator = fetch_data_generator(
            models, 'product.supplierinfo', 'search_read',
            domain=[('product_code', 'in', product_codes)],
            fields=['id'], batch_size=batch_size
        )

        print(f"IDS: {vendor_price_generator}")
    
        headers = ['Vendor Name', 'Product Code', 'Product Template', 'Minimum Quantity', 'Price', 'Currency']
        file_count = 1
        tasks = []
        all_vendor_pricelist_ids = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            print("Preparing to write Excel files...")
            rows = []
            for batch in vendor_price_generator:
                print(f"BATCH: {batch}")
                if not batch:
                    print("Warning: Empty batch received. Skipping...")
                    continue  # Skip processing if batch is empty
                
                batch_ids = [record.get('id') for record in batch if record.get('id') is not None]
                
                if not batch_ids:
                    print("Warning: No valid IDs found in batch. Skipping...")
                    continue  # Skip processing if all IDs are None
                
                print(f"Processing batch with {len(batch_ids)} valid records...")

                vendor_price_lists = fetch_data(
                    models, 'product.supplierinfo', 'search_read',
                    domain=[['id', 'in', batch_ids]],
                    fields=['partner_id', 'product_code', 'product_tmpl_id', 'min_qty', 'price', 'currency_id']
                )

                print(f"vendor_price_lists :  {vendor_price_lists}")
                
                for record in vendor_price_lists:
                    partner_name = record.get('partner_id', ['N/A'])[1] if isinstance(record.get('partner_id'), list) and record.get('partner_id') else 'N/A'
                    product_code = record.get('product_code', 'N/A') or 'N/A'
                    product_name = record.get('product_tmpl_id', ['N/A'])[1] if isinstance(record.get('product_tmpl_id'), list) and record.get('product_tmpl_id') else 'N/A'
                    min_qty = record.get('min_qty', 0.0) or 0.0
                    price = record.get('price', 0.0) or 0.0
                    currency = record.get('currency_id', ['N/A'])[1] if isinstance(record.get('currency_id'), list) and record.get('currency_id') else 'N/A'
                    rows.append([partner_name, product_code, product_name, min_qty, price, currency])

                    print(f"Processed: {partner_name}, {product_code}, {product_name}, {min_qty}, {price}, {currency}")
                    all_vendor_pricelist_ids.append({'id': record.get('id')}) if record.get('id') else False
                    
                    if len(rows) >= max_rows_per_file:
                        file_name = f"Vendor_Pricelist_NET_FILE_BACKUP_{current_date}_Part{file_count}.xlsx"
                        file_path = os.path.join(backup_folder_path, file_name)
                        task = executor.submit(write_excel_file, file_path, rows, headers)
                        tasks.append((task, file_path, file_name))
                        rows = []
                        file_count += 1
                
            if rows:
                file_name = f"Vendor_Pricelist_NET_FILE_BACKUP_{current_date}_Part{file_count}.xlsx"
                file_path = os.path.join(backup_folder_path, file_name)
                task = executor.submit(write_excel_file, file_path, rows, headers)
                tasks.append((task, file_path, file_name))
                
            for task, file_path, file_name in tasks:
                task.result()
                print(f"{file_path}, {file_name}, {file_folder}")
                upload_to_odoo(file_path, file_name, file_folder)

        try:
            for i in range(0, len(all_vendor_pricelist_ids), batch_size):
                batch_ids = all_vendor_pricelist_ids[i:i + batch_size]
                flattened_batch_ids = [record['id'] for record in batch_ids if 'id' in record]
                print(f"Flattened batch_ids: {len(flattened_batch_ids)}")

                # Delete batch from the database
                fetch_data(models, 'product.supplierinfo', 'unlink', ids=flattened_batch_ids)
                print(f"Deleted batch {i // batch_size + 1}: {len(batch_ids)} records.")

            print("All specified vendor price lists deleted successfully.")
        
        except Exception as e:
            print(f"Error deleting records: {e}")
    
    except Exception as e:
        print(f"Error: {e}")
    
# def DELETE_SPECIFIC_VENDOR_PRICELISTS(product_codes, batch_size=20000, file_folder=None, max_rows_per_file=200000):
#     """
#     Deletes specific vendor price list entries based on product codes in batches and writes them concurrently to Excel files.
#     """
#     try:
#         current_date = datetime.now().strftime('%Y-%m-%d_%H%M%S')
        
#         # Parent folder: Spreadsheets
#         parent_folder_path = "Spreadsheets"
#         os.makedirs(parent_folder_path, exist_ok=True)  # Ensure the parent folder exists
        
#         # Subfolder for the current backup: NET_{current_date}
#         backup_folder_path = os.path.join(parent_folder_path, f"NET_PRICEFILE_BACKUP_{current_date}")
#         os.makedirs(backup_folder_path, exist_ok=True)  # Create the subfolder for the backup
        
#         print(f"Fetching specific vendor price list IDs...: {product_codes}")
#         vendor_price_generator = fetch_data_generator(
#             models, 'product.supplierinfo', 'search_read',
#             domain=[('product_code', 'in', product_codes)],
#             fields=['id'], batch_size=batch_size
#         )

#         print(f"IDS: {vendor_price_generator}")
    
#         headers = ['Vendor Name', 'Product Code', 'Product Template', 'Minimum Quantity', 'Price', 'Currency']
#         file_count = 1
#         tasks = []
#         all_vendor_pricelist_ids = []

#         with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
#             print("Preparing to write Excel files...")
#             rows = []
#             for batch in vendor_price_generator:
#                 if not batch:
#                     print("Warning: Empty batch received. Skipping...")
#                     continue  # Skip processing if batch is empty
                
#                 batch_ids = [record.get('id') for record in batch if record.get('id') is not None]
                
#                 if not batch_ids:
#                     print("Warning: No valid IDs found in batch. Skipping...")
#                     continue  # Skip processing if all IDs are None
                
#                 print(f"Processing batch with {len(batch_ids)} valid records...")

#                 vendor_price_lists = fetch_data(
#                     models, 'product.supplierinfo', 'search_read',
#                     domain=[['id', 'in', batch_ids]],
#                     fields=['partner_id', 'product_code', 'product_tmpl_id', 'min_qty', 'price', 'currency_id']
#                 )
                
#                 for record in vendor_price_lists:
#                     partner_name = record.get('partner_id', ['N/A'])[1] if isinstance(record.get('partner_id'), list) and record.get('partner_id') else 'N/A'
#                     product_code = record.get('product_code', 'N/A') or 'N/A'
#                     product_name = record.get('product_tmpl_id', ['N/A'])[1] if isinstance(record.get('product_tmpl_id'), list) and record.get('product_tmpl_id') else 'N/A'
#                     min_qty = record.get('min_qty', 0.0) or 0.0
#                     price = record.get('price', 0.0) or 0.0
#                     currency = record.get('currency_id', ['N/A'])[1] if isinstance(record.get('currency_id'), list) and record.get('currency_id') else 'N/A'
#                     rows.append([partner_name, product_code, product_name, min_qty, price, currency])

#                     print(f"Processed: {partner_name}, {product_code}, {product_name}, {min_qty}, {price}, {currency}")
#                     all_vendor_pricelist_ids.append({'id': record.get('id')}) if record.get('id') else False
                    
#                     if len(rows) >= max_rows_per_file:
#                         file_name = f"Vendor_Pricelist_NET_FILE_BACKUP_{current_date}_Part{file_count}.xlsx"
#                         file_path = os.path.join(backup_folder_path, file_name)
#                         task = executor.submit(write_excel_file, file_path, rows, headers)
#                         tasks.append((task, file_path, file_name))
#                         rows = []
#                         file_count += 1
                
#             if rows:
#                 file_name = f"Vendor_Pricelist_NET_FILE_BACKUP_{current_date}_Part{file_count}.xlsx"
#                 file_path = os.path.join(backup_folder_path, file_name)
#                 task = executor.submit(write_excel_file, file_path, rows, headers)
#                 tasks.append((task, file_path, file_name))
                
#             for task, file_path, file_name in tasks:
#                 task.result()
#                 upload_to_odoo(file_path, file_name, file_folder)

#         try:
#             for i in range(0, len(all_vendor_pricelist_ids), batch_size):
#                 batch_ids = all_vendor_pricelist_ids[i:i + batch_size]
#                 flattened_batch_ids = [record['id'] for record in batch_ids if 'id' in record]
#                 print(f"Flattened batch_ids: {len(flattened_batch_ids)}")

#                 # Delete batch from the database
#                 fetch_data(models, 'product.supplierinfo', 'unlink', ids=flattened_batch_ids)
#                 print(f"Deleted batch {i // batch_size + 1}: {len(batch_ids)} records.")

#             print("All specified vendor price lists deleted successfully.")
        
#         except Exception as e:
#             print(f"Error deleting records: {e}")
    
#     except Exception as e:
#         print(f"Error: {e}")


def fetch_data_generator(models, model, method, domain, fields, batch_size=10000):
    """ Generator function to fetch data in chunks, ensuring no None values are yielded. """
    offset = 0
    while True:
        batch = fetch_data(models, model, method, domain=domain, fields=fields, limit=batch_size, offset=offset) or []
        
        print(f"Debug: Fetched batch at offset {offset}: {batch}")  # Debugging statement

        if not batch:  # If batch is empty, stop iteration
            break
        
        yield batch
        offset += batch_size

def fetch_or_create_folder(folder_name):
    """ Fetches an existing folder by name or creates it if it doesn't exist. """
    # Search for the folder by name
    folder = fetch_data(models, "documents.document", "search", domain=[('name', '=', folder_name)], limit=1)
    
    if folder:
        return folder[0]  # Return the existing folder ID
    else:
        # Create the folder if it doesn't exist
        folder_data = {
            "name": folder_name,
            "parent_id": False  # Top-level folder
        }
        new_folder = fetch_data(models, "documents.folder", "create", values=[folder_data])
        return new_folder[0] if isinstance(new_folder, list) else new_folder

def write_excel_file(file_path, data, headers):
    """ Writes data to an Excel file using xlsxwriter. """
    workbook = xlsxwriter.Workbook(file_path)
    worksheet = workbook.add_worksheet()
    
    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header)
    
    for row_num, record in enumerate(data, start=1):
        for col_num, value in enumerate(record):
            worksheet.write(row_num, col_num, value)
    
    workbook.close()

def upload_to_odoo(file_path, file_name, folder_name):
    """ Uploads the file to Odoo and links it to Documents. """

    folder_id = fetch_or_create_folder(folder_name)

    with open(file_path, "rb") as file:
        file_base64 = base64.b64encode(file.read()).decode()
    
    spreadsheet_data = {
        "name": file_name,
        "type": "binary",
        "datas": file_base64,
        "mimetype": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }
    
    attachment_response = fetch_data(models, "ir.attachment", "create", values=[spreadsheet_data])
    attachment_id = attachment_response[0] if isinstance(attachment_response, list) else attachment_response
    
    # Link the attachment to the document (optional)
    link_data = {"name": file_name, "attachment_id": attachment_id,"folder_id": folder_id}
    fetch_data(models, "documents.document", "create", values=[link_data])


def DELETE_ALL_VENDOR_PRICELISTS(batch_size=20000, file_folder=None, max_rows_per_file=200000):
    """ Deletes all vendor price list entries in batches and writes them concurrently to Excel files. """
    try:
        current_date = datetime.now().strftime('%Y-%m-%d_%H%M%S')
      
        # Parent folder: Spreadsheets
        parent_folder_path = "/var/www/abomar-pmm-api/abomar-pmm/main/odoo/Spreadsheets"
        os.makedirs(parent_folder_path, exist_ok=True)  # Ensure the parent folder exists
        
        # Subfolder for the current backup: FULL_{current_date}
        backup_folder_path = os.path.join(parent_folder_path, f"FULL_PRICEFILE_BACKUP_{current_date}")
        os.makedirs(backup_folder_path, exist_ok=True)  # Create the subfolder for the backup
        
        print("Fetching all vendor price list IDs...")
        vendor_price_generator = fetch_data_generator(models, 'product.supplierinfo', 'search_read', domain=[], fields=['id'], batch_size=batch_size)
    
        headers = ['Vendor Name', 'Product Code', 'Product Template', 'Minimum Quantity', 'Price', 'Currency']
        file_count = 1
        tasks = []
        all_vendor_pricelist_ids = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            print("Preparing to write Excel files...")
            rows = []
            for batch in vendor_price_generator:
                vendor_price_lists = fetch_data(
                    models, 'product.supplierinfo', 'search_read',
                    domain=[['id', 'in', [record['id'] for record in batch]]],
                    fields=['partner_id', 'product_code', 'product_tmpl_id', 'min_qty', 'price', 'currency_id']
                )
                
                for record in vendor_price_lists:
                    partner_name = record.get('partner_id', ['N/A'])[1] if isinstance(record.get('partner_id'), list) else 'N/A'
                    product_code = record.get('product_code', 'N/A')
                    product_name = record.get('product_tmpl_id', ['N/A'])[1] if isinstance(record.get('product_tmpl_id'), list) else 'N/A'
                    min_qty = record.get('min_qty', 0.0)
                    price = record.get('price', 0.0)
                    currency = record.get('currency_id', ['N/A'])[1] if isinstance(record.get('currency_id'), list) else 'N/A'
                    rows.append([partner_name, product_code, product_name, min_qty, price, currency])
                    all_vendor_pricelist_ids.append({'id': record.get('id')})
                    
                    if len(rows) >= max_rows_per_file:
                        file_name = f"Vendor_Pricelist_FULL_FILE_BACKUP_{current_date}_Part{file_count}.xlsx"
                        file_path = os.path.join(backup_folder_path, file_name)
                        task = executor.submit(write_excel_file, file_path, rows, headers)
                        tasks.append((task, file_path, file_name))
                        rows = []
                        file_count += 1
                
            if rows:
                file_name = f"Vendor_Pricelist_FULL_FILE_BACKUP_{current_date}_Part{file_count}.xlsx"
                file_path = os.path.join(backup_folder_path, file_name)
                task = executor.submit(write_excel_file, file_path, rows, headers)
                tasks.append((task, file_path, file_name))
                
            for task, file_path, file_name in tasks:
                task.result()
                upload_to_odoo(file_path, file_name, file_folder)

        try:
            for i in range(0, len(all_vendor_pricelist_ids), batch_size):
                batch_ids = all_vendor_pricelist_ids[i:i + batch_size]
                flattened_batch_ids = [record['id'] for record in batch_ids if 'id' in record]
                print(f"Flattened batch_ids: {len(flattened_batch_ids)}")

                # Delete batch from the database
                fetch_data(models, 'product.supplierinfo', 'unlink', ids=flattened_batch_ids)
                print(f"Deleted batch {i // batch_size + 1}: {len(batch_ids)} records.")

            print("All vendor price lists deleted successfully.")
        
        except Exception as e:
            print(f"Error deleting records: {e}")
    
    except Exception as e:
        print(f"Error: {e}")
# def DELETE_ALL_VENDOR_PRICELISTS(batch_size=20000, max_rows=1000000):
#     """
#     Deletes all vendor price list entries in batches of a given size.
#     Exports the data to a spreadsheet and uploads it to Odoo Documents before deletion.
#     """
#     try:
#         current_date = datetime.now().strftime('%Y-%m-%d_%H%M%S')
#         spreadsheet_file_name = f"Vendor_Pricelist_FULL_BACKUP_{current_date}.csv"
#         folder_path = "Spreadsheet"
#         file_path = os.path.join(folder_path, spreadsheet_file_name)

#         if not os.path.exists(folder_path):
#             os.makedirs(folder_path)

#         print("Fetching all vendor price list IDs...")
#         all_vendor_pricelist_ids = fetch_data(
#             models,
#             'product.supplierinfo',
#             'search_read',
#             domain=[],
#             fields=['id']
#         )

#         total_records = len(all_vendor_pricelist_ids)
#         print(f"Total vendor price lists to delete: {total_records}")

#         if total_records == 0:
#             print("No records found to delete.")
#             return

#         # Prepare spreadsheet data
#         rows = [['Vendor Name', 'Product Code', 'Product Template', 'Minimum Quantity', 'Price', 'Currency']]

#         vendor_price_lists = fetch_data(
#             models,
#             'product.supplierinfo',
#             'search_read',
#             domain=[['id', 'in', [record['id'] for record in all_vendor_pricelist_ids]]],
#             fields=['partner_id', 'product_code', 'product_tmpl_id', 'min_qty', 'price', 'currency_id']
#         )
       
#         for record in vendor_price_lists:
#             if len(rows) >= max_rows:
#                 print(f"Reached the maximum row limit of {max_rows}. Stopping data collection.")
#                 break

#             partner_name = record.get('partner_id', ['N/A'])[1] if isinstance(record.get('partner_id'), list) else 'N/A'
#             product_code = record.get('product_code', 'N/A')
#             product_name = record.get('product_tmpl_id', ['N/A'])[1] if isinstance(record.get('product_tmpl_id'), list) else 'N/A'
#             min_qty = record.get('min_qty', 0.0)
#             price = record.get('price', 0.0)
#             currency = record.get('currency_id', ['N/A'])[1] if isinstance(record.get('currency_id'), list) else 'N/A'

#             rows.append([partner_name, product_code, product_name, min_qty, price, currency])

#         print(f"vendor_price_lists : {rows}")

#         # Save CSV file to disk
#         with open(file_path, mode="w", newline="", encoding="utf-8") as file:
#             writer = csv.writer(file)
#             for row_data in rows:
#                 print(f"Writing row: {row_data}")  # Debugging statement
#                 writer.writerow(row_data)

#         # Convert file to base64 for Odoo upload
#         with open(file_path, "rb") as file:
#             file_base64 = base64.b64encode(file.read()).decode()

#         # Prepare data for Odoo attachment
#         spreadsheet_data = {
#             "name": spreadsheet_file_name,
#             "type": "binary",
#             "datas": file_base64,
#             "mimetype": "text/csv"
#         }

#         # Create the attachment in Odoo
#         attachment_response = fetch_data(models, "ir.attachment", "create", values=[spreadsheet_data])
#         attachment_id = attachment_response[0] if isinstance(attachment_response, list) else attachment_response

#         # Link the attachment to Odoo Documents
#         link_data = {
#             "name": spreadsheet_file_name,
#             "attachment_id": attachment_id
#         }

#         fetch_data(models, "documents.document", "create", values=[link_data])

    #     # Proceed with batch deletion
    #     try:
    #         batch_size = 20000  # Adjust batch size as needed
    #         for i in range(0, len(all_vendor_pricelist_ids), batch_size):
    #             batch_ids = all_vendor_pricelist_ids[i:i + batch_size]
    #             flattened_batch_ids = [record['id'] for record in batch_ids if 'id' in record]
    #             print(f"Flattened batch_ids: {len(flattened_batch_ids)}")

    #             # Delete batch from the database
    #             fetch_data(models, 'product.supplierinfo', 'unlink', ids=flattened_batch_ids)
    #             print(f"Deleted batch {i // batch_size + 1}: {len(batch_ids)} records.")

    #         print("All vendor price lists deleted successfully.")

    #     except Exception as e:
    #         print(f"Error deleting records: {e}")

    # except Exception as e:
    #     print(f"Error during batch deletion of vendor price lists: {e}")


def parse_dat_line(line):
    product_id = line[0:18].strip()  
    new_price = line[57:72].strip() 

    new_price = new_price.lstrip('0')

    if new_price == '':
        new_price = '0'

    if new_price.startswith('.'):
        new_price = '0' + new_price

    # Build the product dictionary based on parsed data
    return {
        "id": product_id,
        "price": new_price,  # Add other fields based on .DAT file structure
    }

def write_to_json(data, output_json_path, overwrite=False):
    if overwrite:
        # Overwrite the existing file with the new data
        with open(output_json_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
    else:
        # Append the data to the existing file
        try:
            with open(output_json_path, 'r') as json_file:
                existing_data = json.load(json_file)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []

        existing_data.extend(data)

        with open(output_json_path, 'w') as json_file:
            json.dump(existing_data, json_file, indent=4)

def UPDATE_VENDOR_PRICELIST(output_json_path):
    try:
        # Load the JSON data containing the product prices
        with open(output_json_path, 'r') as f:
            matched_products = json.load(f)

        # Ensure matched_products is a list of dictionaries
        if not isinstance(matched_products, list):
            raise ValueError("JSON data must be an array of products.")

        # Loop through the products and update their prices
        for product in matched_products:
            if isinstance(product, dict):
                product_id = product.get("id")  # Product template ID (AA5122R)
                price = product.get("price")  # Price value

                # Skip if product_id or price is not found
                if not product_id or not price:
                    print(f"Missing product data: {product}")
                    continue

                # Find the product template ID from the product code
                product_template_ids = fetch_data(
                    models, 'product.template', 'search',
                    [[('default_code', '=', product_id)]]
                )

                # If product template is found, update its price
                if product_template_ids:
                    # Assuming price is stored in vendor pricelist (can be adjusted for other models)
                    pricelist_id = 1  # Replace with actual pricelist ID
                    pricelist_item_ids = fetch_data(
                        models, 'vendor.pricelist', 'search',
                        [[('product_tmpl_id', '=', product_template_ids[0])]]
                    )

                    # If there's an existing pricelist item, update it
                    if pricelist_item_ids:
                        fetch_data(
                            models, 'vendor.pricelist', 'write',
                            [pricelist_item_ids, {'price': float(price)}]
                        )
                        print(f"Updated price for product {product_id} in pricelist.")
                    else:
                        print(f"Created new pricelist item for product {product_id}.")
                else:
                    print(f"Product {product_id} not found in Odoo.")
            else:
                print(f"Unexpected data structure: {product}")
    except xmlrpc.client.Fault as e:
        print(f"XML-RPC Fault: {e}")
    except Exception as e:
        print(f"Error: {e}")

def get_model_fields():
    #change for specific model
    model_name = 'product.product'

    fields = models.execute_kw(
        db, uid, password,
        model_name, 'fields_get',
        [],
        {'attributes': ['string', 'type', 'help']}
    )

    # Print the fields and their details
    for field_name, field_info in fields.items():
        print(f"Field: {field_name}")
        print(f"  Label: {field_info['string']}")
        print(f"  Type: {field_info['type']}")
        print(f"  Help: {field_info.get('help', 'No help text available')}")
        print("-" * 50)

## in replenishment once inserted , automatically click the order function then load in Receipts and Deliveries.  If same destination and origin , 1 row of records. if not then each should have rows

## In pricefile , need to insert in spreadsheet before deleting records when loading FULL file
# In Net file , no need to insert in spreadsheet
# In pricefile , Delete all records when loading FULL file
# In net file , instead of updating records , delete the row record then insert new
