from odoo_configuration import get_server_proxy, fetch_data, db, username, password, uid, url, models, common
from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import defaultdict
import re
import time
import random
import xmlrpc.client
import shutil
import os
import json

output_json_path = "matched_products.json"

def GET_PRODUCT_TMPL_ID(product_code):
    category_name = 'John Deere'
    products = fetch_data(models, 'product.product', 'search_read',
                          domain=[('default_code', '=', product_code), ('categ_id.name', '=', category_name)],
                          fields=['product_tmpl_id'])
    
    # Ensure that products is a list and we correctly access the 'product_tmpl_id' key
    if products and isinstance(products, list) and len(products) > 0:
        product_tmpl_id = products[0].get('product_tmpl_id')
        if product_tmpl_id:
            # If product_tmpl_id is a list, return the first element
            if isinstance(product_tmpl_id, list):
                return product_tmpl_id[0]
            # If product_tmpl_id is not a list, return it directly
            return product_tmpl_id
    return None

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
        formatted_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        return formatted_date
    except ValueError as e:
        print(f"NO EFFECTIVE DATE FOUND: {e}")
        formatted_date = datetime.strptime(effective_date, "%Y%m%d").strftime("%Y-%m-%d")
        return formatted_date
    
def PROCESS_BY_BATCH(data_batch, retries=5):

    """
    Process a batch of data, retrying if rate limit is exceeded (HTTP 429).
    """
    attempt = 0
    while attempt <= retries:
        try:
            for data in data_batch:
                product_code = data['product_code']
                price = data['price']
                JD_list_price = data['x_studio_float_field_3ma_1ic57q8n5']
                start_date = data['date_start']
                # Check if product_code exists
                existing_product = fetch_data(
                    models, 
                    'product.supplierinfo', 
                    'search', 
                    [['product_code', '=', product_code]]
                )
                print(f"Existing product: {existing_product}")

                if existing_product:
                    product_id = existing_product[0]

                    result = fetch_data(
                        models,
                        'product.supplierinfo',
                        'write',
                        ids=[product_id],
                        values={'price': price, 'x_studio_float_field_3ma_1ic57q8n5': JD_list_price, 'date_start': start_date}
                    )
                    print(f"Update result: {result}")
                    print(f"Updated product {product_code} (ID: {product_id})")
                else:
                    fetch_data(
                        models,
                        'product.supplierinfo',
                        'create',
                        values=[data]
                    )
                    print(f"Created new product {product_code}")

            print("Batch processed successfully.")
            return True
        except xmlrpc.client.ProtocolError as e:
            if e.errcode == 429:
                print(f"Attempting to retry {attempt}.")
                attempt += 1
                wait_time = 2 ** attempt + random.uniform(0, 1)
                time.sleep(wait_time)
            else:
                raise
        except Exception as e:
            raise

    return None

def PROCESS_DAT_PRICE_FILE(directory, batch_size=20000):
    """
    Process DAT price file in batches, retrying if rate limits are encountered.
    """
    batch = []
    
    try:
        for filename in os.listdir(directory):
            if filename.endswith(".DAT"):
                file_path = os.path.join(directory, filename)

                with open(file_path, 'r') as dat_file:
                    header = next(dat_file)  # Skip header
                    effective_date = header[54:62]
                    for line in dat_file:
                        # Extract product data from the line
                        product_id = line[0:17].strip()
                        product_name = line[24:51].strip().lstrip('-')
                        start_date = line[208:216].strip()
                        quantity = line[162].strip()
                        price = line[56:72].strip()
                        vendor = 79  # Vendor ID
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
                        
                        start_date = FORMAT_DATE(start_date, effective_date)
                        today = datetime.today().date()

                        if start_date:
                            start_date_date = datetime.strptime(start_date, "%Y-%m-%d").date()
                            if start_date_date >= today:
                                print(f"Adding this product {product_name}")
                                # Prepare data for the batch
                                batch.append({
                                    'partner_id': vendor,
                                    'product_name': product_name,
                                    'product_code': product_id,
                                    'product_tmpl_id': False,
                                    'x_studio_float_field_3ma_1ic57q8n5': float(JD_list_price),
                                    'price': float(price),
                                    'min_qty': 1 if quantity == 'E' else 100 if quantity == 'C' else int(quantity) if quantity.isdigit() else 1,
                                    'currency_id': 1,
                                    'date_start': start_date,
                                })

                        if len(batch) >= batch_size:
                            print(f"Batch size reached: {len(batch)}. Sending to process...")
                            if PROCESS_BY_BATCH(batch):
                                batch.clear()
                            else:
                                print("Failed to process the batch.")
                                return False
                            

        if batch:
            if PROCESS_BY_BATCH(batch):
                print("Remaining batch processed successfully.")
            else:
                print("Failed to process remaining batch.")
                return False
        print(f"File {file_path} processed successfully. Deleting file...")
        os.remove(file_path)

        print("All products loaded successfully.")
        return True  # Success if all batches are processed

    except Exception as e:
        print(f"Error processing DAT price file: {e}")
        return False  # Return False if an error occurs

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
    model_name = 'res.currency'

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
