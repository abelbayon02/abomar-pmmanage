#!/usr/bin/env python3
import xmlrpc.client
import json
import logging
import os
import requests
import datetime
import yagmail
from dotenv import load_dotenv
#from datetime import timedelta
from dateutil.relativedelta import relativedelta
from requests.auth import HTTPBasicAuth

load_dotenv()
# --- Odoo Configuration ---
url = os.getenv("PROD_ODOO_URL")
db = os.getenv("PROD_ODOO_DB")
username = os.getenv("PROD_ODOO_USERNAME")
password = os.getenv("PROD_ODOO_PASSWORD")

# --- XML-RPC Connection ---
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

## UPLOAD AND DOWNLOAD TOKENS PRE-REQUISITES ##
client_id = os.getenv("PROD_CLIENT_ID")
client_secret = os.getenv("PROD_CLIENT_SECRET")
token_url = os.getenv("PROD_TOKEN_URL")
grant_type = os.getenv("PROD_GRANT_TYPE")
upload_api_url = os.getenv("PROD_JD_UPLOAD_API_URL")

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

## UPLOAD AND DOWNLOAD TOKENS PRE-REQUISITES ##




log_filename = "/var/www/abomar-pmm-api/abomar-pmm/main/pmm/PMM.log"  # Constant log file name
logging.basicConfig(
    filename=log_filename,
    filemode="a",  # Append mode: new log entries are added to the existing file
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("Script execution started.")


# ---------------------------
# Helper Functions
# ---------------------------
def get_target_locations():
    """
    Returns a mapping from group name to a list of location IDs.
    Groups are determined by whether the location's name contains:
      - 'CEB' → Cebu
      - 'PAS' → Pasig
      - 'DAV' → Davao City
      - 'BAC' → Bacolod
      - 'CAG' → Cagayan de oro
    """
    domain = [
        '|', '|', '|', '|',
        ('name', 'ilike', 'CEB'),
        ('name', 'ilike', 'PAS'),
        ('name', 'ilike', 'DAV'),
        ('name', 'ilike', 'BAC'),
        ('name', 'ilike', 'CAG'),
    ]
    locations = models.execute_kw(
        db, uid, password,
        'stock.location', 'search_read',
        [domain],
        {'fields': ['id', 'name']}
    )
    # Ensure groups match exactly the warehouse names used in Sales
    groups = {"Cebu": [], "Pasig": [], "Davao City": [], "Bacolod": [], "Cagayan de oro": []}
    if not locations:
        return groups

    for loc in locations:
        name = loc['name'].upper()
        if 'CEB' in name:
            groups["Cebu"].append(loc['id'])
        elif 'PAS' in name:
            groups["Pasig"].append(loc['id'])
        elif 'DAV' in name:
            groups["Davao City"].append(loc['id'])
        elif 'BAC' in name:
            groups["Bacolod"].append(loc['id'])
        elif 'CAG' in name:
            groups["Cagayan de oro"].append(loc['id'])
    return groups

def get_warehouse_mapping():
    """
    Returns a mapping from location group to a warehouse code substring
    used in sale.order filtering.
    """
    return {
        "Cebu": "CEB",
        "Pasig": "PAS",
        "Davao City": "DAV",
        "Bacolod": "BAC",
        "Cagayan de oro": "CAG"
    }

def __warehouse_codes(name):

    codes = {
        "CEB": "7A1758",
        "PAS": "7ASC58",
        "DAV": "7ASB58",
        "BAC": "7ASE58",
        "CAG": "7ASD58"
    }

    code_value = codes.get(name)  # Get the code for the given name

    return code_value[-4:] if code_value else None  # Extract the last 4 characters if found

def get_month_end_dates(start_date, end_date):
    """
    Generate a list of month-end dates between start_date and end_date.
    Assumes start_date is the first day of a month.
    """
    dates = []
    current = start_date
    while current <= end_date:
        next_month = current.replace(day=28) + datetime.timedelta(days=4)
        month_end = next_month - datetime.timedelta(days=next_month.day)
        dates.append(month_end)
        current = month_end + datetime.timedelta(days=1)
    return dates

def get_last_month_range():
    """
    Returns (start, end) dates for the last full calendar month.
    """
    today = datetime.date.today()
    first_day_current = today.replace(day=1)
    last_day_last_month = first_day_current - datetime.timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    return (first_day_last_month, last_day_last_month)

def get_period_range(period_type):
    """
    Returns (start, end) dates for a 12-month period.
    If period_type == "last_12": period from 12 months ago (using last month) to last month.
    If period_type == "prior_12": the 12-month period immediately preceding that.
    For example, if today is March 3, 2025:
      - Last Month: Feb 1, 2025 – Feb 28, 2025
      - Last 12 Months: Feb 1, 2024 – Feb 28, 2025
      - Prior 12 Months: Feb 1, 2023 – Jan 31, 2024
    """
    last_month_start, last_month_end = get_last_month_range()
    if period_type == "last_12":
        start = last_month_start.replace(year=last_month_start.year - 1)
        end = last_month_end
    elif period_type == "prior_12":
        last12_start = last_month_start.replace(year=last_month_start.year - 1)
        start = last12_start.replace(year=last12_start.year - 1)
        end = last12_start - datetime.timedelta(days=1)
    else:
        raise ValueError("Invalid period type")
    return (start, end)

# ---------------------------
# Inventory Valuation Functions
# ---------------------------
def get_inventory_valuation_for_location(location_ids, ref_date_str):
    """
    For the given location IDs, this function calculates the current inventory value by:
      1. Retrieving stock.quant records and summing the on-hand quantity per product.
      2. Querying product.product for the filtered products (only those with:
            - Category name containing "John Deere"
            - A non-empty default_code)
         to retrieve the product's average cost (avg_cost).
      3. Calculating the total value as the sum of (on-hand quantity * avg_cost) for each product.
    The ref_date_str (formatted as "YYYY-MM-DD 23:59:59") is used to evaluate historical costs.
    """
    domain = [('location_id', 'in', location_ids)]
    quants = models.execute_kw(
        db, uid, password,
        'stock.quant', 'search_read',
        [domain],
        {'fields': ['product_id', 'quantity']}
    )
    if not quants:
        return 0.0

    product_qty = {}
    for quant in quants:
        prod = quant.get('product_id')
        prod_id = prod[0] if isinstance(prod, list) else prod
        product_qty[prod_id] = product_qty.get(prod_id, 0.0) + quant.get('quantity', 0.0)
    if not product_qty:
        return 0.0
    product_ids = list(product_qty.keys())

    products = models.execute_kw(
        db, uid, password,
        'product.product', 'search_read',
        [[
            ('id', 'in', product_ids),
            ('categ_id.name', 'ilike', 'John Deere'),
            ('default_code', '!=', False),
            ('default_code', '!=', '')
        ]],
        {'fields': ['id', 'avg_cost'], 'context': {'to_date': ref_date_str}}
    )
    if not products:
        return 0.0

    total_value = 0.0
    for prod in products:
        prod_id = prod['id']
        qty = product_qty.get(prod_id, 0.0)
        cost = prod.get('avg_cost', 0.0)
        total_value += qty * cost
    return total_value

def average_inventory_valuation(location_ids, month_end_dates):
    """
    Given a list of month-end dates, compute the inventory valuation at each month-end
    for the given locations and return their average.
    """
    total = 0.0
    count = 0
    for dt in month_end_dates:
        ref_str = dt.strftime("%Y-%m-%d 23:59:59")
        val = get_inventory_valuation_for_location(location_ids, ref_str)
        total += val
        count += 1
    return (total / count) if count > 0 else 0.0

# ---------------------------
# Sales Functions
# ---------------------------
# def order_contains_john_deere(order_line_ids):
#     """
#     Check if at least one sale.order.line in order_line_ids has a product with:
#       - Category name containing "John Deere"
#       - A non-empty default_code.
#     """
#     lines = models.execute_kw(
#         db, uid, password,
#         'sale.order.line', 'search_read',
#         [[('id', 'in', order_line_ids)]],
#         {'fields': ['product_id']}
#     )
#     if not lines:
#         return False
#     prod_ids = [line['product_id'][0] for line in lines if line.get('product_id')]
#     if not prod_ids:
#         return False
#     john_deere_products = models.execute_kw(
#         db, uid, password,
#         'product.product', 'search_read',
#         [[
#             ('id', 'in', prod_ids),
#             ('categ_id.name', 'ilike', 'John Deere'),
#             ('default_code', '!=', False),
#             ('default_code', '!=', '')
#         ]],
#         {'fields': ['id']}
#     )
#     return bool(john_deere_products)

# def get_total_sales_for_location(warehouse_code, start_date, end_date):
#     """
#     Query sale.order for orders (state in ['sale', 'done']) with date_order between start_date and end_date
#     and whose warehouse_id.name contains the given warehouse_code.
#     Only include orders that have at least one order line with a product that passes the John Deere filter.
#     Returns the sum of amount_total.
#     """
#     domain = [
#         ('state', 'in', ['sale', 'done']),
#         ('date_order', '>=', start_date.strftime('%Y-%m-%d')),
#         ('date_order', '<=', end_date.strftime('%Y-%m-%d')),
#         ('warehouse_id.name', 'ilike', warehouse_code)
#     ]
#     orders = models.execute_kw(
#         db, uid, password,
#         'sale.order', 'search_read',
#         [domain],
#         {'fields': ['amount_total', 'order_line']}
#     )
#     if not orders:
#         return 0.0
#     total = 0.0
#     for order in orders:
#         order_line_ids = order.get('order_line', [])
#         if order_line_ids and order_contains_john_deere(order_line_ids):
#             total += order.get('amount_total', 0.0)
#     return total

# ---------------------------
# Revised Sales Functions (John Deere only)
# ---------------------------
def get_total_sales_for_location_john_deere(warehouse_code, start_date, end_date):
    """
    Query sale.order for orders (state in ['sale', 'done']) with date_order between start_date and end_date
    and whose warehouse_id.name contains the given warehouse_code.
    For each order, only the sale.order.lines that have a product in the John Deere category (with a valid default_code)
    are summed using their price_subtotal value.
    """
    domain = [
        ('state', 'in', ['sale', 'done']),
        ('date_order', '>=', start_date.strftime('%Y-%m-%d')),
        ('date_order', '<=', end_date.strftime('%Y-%m-%d')),
        ('warehouse_id.name', 'ilike', warehouse_code)
    ]
    orders = models.execute_kw(
        db, uid, password,
        'sale.order', 'search_read',
        [domain],
        {'fields': ['order_line']}
    )
    total = 0.0
    if orders:
        for order in orders:
            order_line_ids = order.get('order_line', [])
            if order_line_ids:
                # Retrieve only the order lines for John Deere products
                john_deere_lines = models.execute_kw(
                    db, uid, password,
                    'sale.order.line', 'search_read',
                    [[
                        ('id', 'in', order_line_ids),
                        ('product_id.categ_id.name', 'ilike', 'John Deere'),
                        ('product_id.default_code', '!=', False),
                        ('product_id.default_code', '!=', '')
                    ]],
                    {'fields': ['price_subtotal']}
                )
                for line in john_deere_lines:
                    total += line.get('price_subtotal', 0.0)
    return total

def get_unique_customers_for_location_john_deere(warehouse_code, start_date, end_date):
    """
    Query sale.order for orders (state in ['sale', 'done']) with date_order between start_date and end_date
    and whose warehouse_id.name contains the given warehouse_code.
    Only count the order’s partner_id if at least one sale.order.line in the order
    corresponds to a John Deere product (with valid default_code).
    """
    domain = [
        ('state', 'in', ['sale', 'done']),
        ('date_order', '>=', start_date.strftime('%Y-%m-%d')),
        ('date_order', '<=', end_date.strftime('%Y-%m-%d')),
        ('warehouse_id.name', 'ilike', warehouse_code)
    ]
    orders = models.execute_kw(
        db, uid, password,
        'sale.order', 'search_read',
        [domain],
        {'fields': ['order_line', 'partner_id']}
    )
    unique_customers = set()
    if orders:
        for order in orders:
            order_line_ids = order.get('order_line', [])
            if order_line_ids:
                john_deere_lines = models.execute_kw(
                    db, uid, password,
                    'sale.order.line', 'search_read',
                    [[
                        ('id', 'in', order_line_ids),
                        ('product_id.categ_id.name', 'ilike', 'John Deere'),
                        ('product_id.default_code', '!=', False),
                        ('product_id.default_code', '!=', '')
                    ]],
                    {'fields': ['id']}
                )
                if john_deere_lines:
                    partner = order.get('partner_id')
                    if isinstance(partner, list):
                        unique_customers.add(partner[0])
                    elif partner:
                        unique_customers.add(partner)
    return len(unique_customers)

# ---------------------------
# Customer Functions
# ---------------------------
# def get_unique_customers_for_location(warehouse_code, start_date, end_date):
#     """
#     Query sale.order for orders with date_order between start_date and end_date and warehouse_id.name containing warehouse_code.
#     Return the count of unique partner_id.
#     """
#     domain = [
#         ('state', 'in', ['sale', 'done']),
#         ('date_order', '>=', start_date.strftime('%Y-%m-%d')),
#         ('date_order', '<=', end_date.strftime('%Y-%m-%d')),
#         ('warehouse_id.name', 'ilike', warehouse_code)
#     ]
#     orders = models.execute_kw(
#         db, uid, password,
#         'sale.order', 'search_read',
#         [domain],
#         {'fields': ['partner_id']}
#     )
#     if not orders:
#         return 0
#     unique_customers = set()
#     for order in orders:
#         partner = order.get('partner_id')
#         if isinstance(partner, list):
#             unique_customers.add(partner[0])
#         elif partner:
#             unique_customers.add(partner)
#     return len(unique_customers)


# ---------------------------
# Inventory with No Sales Function
# ---------------------------
# ---------------------------
# Revised Inventory with No Sales Function
# ---------------------------
def get_inventory_with_no_sales_for_location(location_ids, warehouse_code, sales_start, sales_end, ref_date_str):
    """
    For the given location_ids, retrieves all product IDs via stock.quant.
    Then filters to include only products that:
      - Belong to a category containing "John Deere"
      - Have a non-empty default_code (active parts)
      - Have been in inventory for more than 12 months 
        (i.e. the product's create_date is older than ref_date minus 12 months)
    Next, it queries sale orders (and corresponding sale.order.line) for the 12-month period [sales_start, sales_end]
    for the given warehouse, and collects all product IDs that had sales during that period.
    The Inventory with No Sales Value is then computed (using context {'to_date': ref_date_str}) as the total valuation 
    of products that did NOT have any sales during the period, calculated as on-hand quantity * avg_cost.
    """
    domain = [('location_id', 'in', location_ids)]
    quants = models.execute_kw(
        db, uid, password,
        'stock.quant', 'search_read',
        [domain],
        {'fields': ['product_id']}
    )
    if not quants:
        return 0.0
    location_product_ids = set()
    for quant in quants:
        prod = quant.get('product_id')
        if isinstance(prod, list):
            location_product_ids.add(prod[0])
        else:
            location_product_ids.add(prod)
    
    threshold_date = datetime.datetime.strptime(ref_date_str, "%Y-%m-%d %H:%M:%S") - relativedelta(years=1)
    threshold_str = threshold_date.strftime("%Y-%m-%d %H:%M:%S")
    
    filtered = models.execute_kw(
        db, uid, password,
        'product.product', 'search_read',
        [[
            ('id', 'in', list(location_product_ids)),
            ('categ_id.name', 'ilike', 'John Deere'),
            ('default_code', '!=', False),
            ('default_code', '!=', ''),
            ('create_date', '<=', threshold_str)
        ]],
        {'fields': ['id']}
    )
    if not filtered:
        return 0.0
    filtered_product_ids = {prod['id'] for prod in filtered}
    
    sale_domain = [
        ('state', 'in', ['sale', 'done']),
        ('date_order', '>=', sales_start.strftime('%Y-%m-%d')),
        ('date_order', '<=', sales_end.strftime('%Y-%m-%d')),
        ('warehouse_id.name', 'ilike', warehouse_code)
    ]
    orders = models.execute_kw(
        db, uid, password,
        'sale.order', 'search_read',
        [sale_domain],
        {'fields': ['order_line']}
    )
    sold_product_ids = set()
    if orders:
        order_line_ids = []
        for order in orders:
            order_line_ids.extend(order.get('order_line', []))
        if order_line_ids:
            lines = models.execute_kw(
                db, uid, password,
                'sale.order.line', 'search_read',
                [[('id', 'in', order_line_ids)]],
                {'fields': ['product_id']}
            )
            for line in lines:
                prod = line.get('product_id')
                if isinstance(prod, list):
                    sold_product_ids.add(prod[0])
                else:
                    sold_product_ids.add(prod)
    
    no_sales_product_ids = filtered_product_ids - sold_product_ids
    if not no_sales_product_ids:
        return 0.0
    
    products = models.execute_kw(
        db, uid, password,
        'product.product', 'search_read',
        [[('id', 'in', list(no_sales_product_ids))]],
        {'fields': ['qty_available', 'avg_cost'], 'context': {'to_date': ref_date_str}}
    )
    if not products:
        return 0.0
    total_value = 0.0
    for prod in products:
        qty = prod.get('qty_available', 0.0)
        cost = prod.get('avg_cost', 0.0)
        total_value += qty * cost
    return total_value
# ---------------------------
# Revised Parts Cost of Sales Function (COS)
# ---------------------------
def get_parts_cost_of_sales_for_location(warehouse_code, start_date, end_date, ref_date_str):
    """
    Calculate the Cost of Sales (COS) for parts with confirmed sales over the given period.
    
    Approach:
      1. Query sale.order for orders (state in ['sale','done']) with date_order between start_date and end_date
         and whose warehouse_id.name contains the given warehouse_code.
      2. Collect all sale order line IDs from these orders.
      3. Query sale.order.line records (fields: product_id, product_uom_qty).
      4. Aggregate the sold quantity per product.
      5. Filter these products to include only those that are John Deere parts (categ_id.name contains 'John Deere'
         and a non-empty default_code).
      6. For each product, query product.product (using context with ref_date_str) to obtain its avg_cost.
      7. Compute the COS as the sum (sold quantity * avg_cost) across all qualifying products.
    """
    # 1. Get sale orders for the given warehouse and period
    order_domain = [
        ('state', 'in', ['sale', 'done']),
        ('date_order', '>=', start_date.strftime('%Y-%m-%d')),
        ('date_order', '<=', end_date.strftime('%Y-%m-%d')),
        ('warehouse_id.name', 'ilike', warehouse_code)
    ]
    orders = models.execute_kw(
        db, uid, password,
        'sale.order', 'search_read',
        [order_domain],
        {'fields': ['order_line']}
    )
    if not orders:
        return 0.0

    # 2. Collect order_line IDs
    order_line_ids = []
    for order in orders:
        order_line_ids.extend(order.get('order_line', []))
    if not order_line_ids:
        return 0.0

    # 3. Query sale.order.line for product and sold quantity (assuming field 'product_uom_qty' exists)
    sale_lines = models.execute_kw(
        db, uid, password,
        'sale.order.line', 'search_read',
        [[('id', 'in', order_line_ids)]],
        {'fields': ['product_id', 'product_uom_qty']}
    )
    if not sale_lines:
        return 0.0

    # 4. Aggregate sold quantities by product ID
    product_qty = {}
    for line in sale_lines:
        product = line.get('product_id')
        if not product:
            continue
        prod_id = product[0] if isinstance(product, list) else product
        qty = line.get('product_uom_qty', 0.0)
        product_qty[prod_id] = product_qty.get(prod_id, 0.0) + qty
    if not product_qty:
        return 0.0
    product_ids = list(product_qty.keys())

    # 5. Filter products to include only John Deere parts
    products = models.execute_kw(
        db, uid, password,
        'product.product', 'search_read',
        [[
            ('id', 'in', product_ids),
            ('categ_id.name', 'ilike', 'John Deere'),
            ('default_code', '!=', False),
            ('default_code', '!=', '')
        ]],
        {'fields': ['id', 'avg_cost'], 'context': {'to_date': ref_date_str}}
    )
    if not products:
        return 0.0

    # 6. Build a mapping from product id to avg_cost
    cost_map = {prod['id']: prod.get('avg_cost', 0.0) for prod in products}

    # 7. Calculate COS as the sum (sold quantity * avg_cost)
    total_cost = 0.0
    for prod_id, qty in product_qty.items():
        avg_cost = cost_map.get(prod_id, 0.0)
        total_cost += qty * avg_cost

    return total_cost

# ---------------------------
# Main Reporting Function
# ---------------------------
def main():
    inv_groups = get_target_locations()    # mapping: group -> list of location IDs
    wh_mapping = get_warehouse_mapping()     # mapping: group -> warehouse code substring
    
    if not inv_groups:
        print("No target locations found for inventory valuation.")
        return

    # Define date ranges
    last_month_start, last_month_end = get_last_month_range()
    last12_start = last_month_start.replace(year=last_month_start.year - 1)
    last12_end = last_month_end
    prior12_start = last12_start.replace(year=last12_start.year - 1)
    prior12_end = last12_start - datetime.timedelta(days=1)


    ref_date_str = last_month_end.strftime("%Y-%m-%d 23:59:59")
    
    # Generate month-end dates for the periods.
    last12_month_ends = get_month_end_dates(last12_start, last12_end)
    prior12_month_ends = get_month_end_dates(prior12_start, prior12_end)
    
    results = []

    for group in inv_groups.keys():
        wh_code = wh_mapping.get(group, "")
        loc_ids = inv_groups[group]
        print(wh_code)
        group_data = {
            "location": group,
            "warehouse_code": wh_code,
            "data": [
                {"metric": "avg_monthly_inventory_value_last_12_months", "value": average_inventory_valuation(loc_ids, last12_month_ends)},
                {"metric": "avg_monthly_inventory_value_prior_12_months", "value": average_inventory_valuation(loc_ids, prior12_month_ends)},
                {"metric": "total_part_sales_last_12_months", "value": get_total_sales_for_location_john_deere(wh_code, last12_start, last12_end)},
                {"metric": "total_part_sales_prior_12_months", "value": get_total_sales_for_location_john_deere(wh_code, prior12_start, prior12_end)},
                {"metric": "total_part_sales_last_month", "value": get_total_sales_for_location_john_deere(wh_code, last_month_start, last_month_end)},
                {"metric": "total_parts_cost_of_sales_last_12_months", "value": get_parts_cost_of_sales_for_location(group, last12_start, last12_end, ref_date_str)},
                {"metric": "total_parts_cost_of_sales_prior_12_months", "value": get_parts_cost_of_sales_for_location(group, prior12_start, prior12_end, ref_date_str)},
                {"metric": "total_parts_cost_of_sales_last_month", "value": get_parts_cost_of_sales_for_location(group, last_month_start, last_month_end, ref_date_str)},
                {"metric": "current_inventory_value", "value": get_inventory_valuation_for_location(loc_ids, last_month_end.strftime("%Y-%m-%d 23:59:59"))},
                {"metric": "inventory_with_no_sales", "value": get_inventory_with_no_sales_for_location(loc_ids, wh_code, last_month_start, last_month_end, last_month_end.strftime("%Y-%m-%d 23:59:59"))},
                {"metric": "total_unique_customers_last_12_months", "value": get_unique_customers_for_location_john_deere(wh_code, last_month_start, last_month_end)}
            ]
        }

        results.append(group_data)

    pmm_data_result = results
    # print(json.dumps(results, indent=4))

    warehouse_codes = ['CEB', 'PAS', 'DAV', 'BAC', 'CAG']
    rows_codes = ['0', 'I', 'J', 'K', 'L','M', 'N', 'O', 'P', 'Q','R', 'S', 'T', 'U']
    __prev_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y%m")

    __pmmdata = []
    __four_bytes_DTF_Account = ''
    for warehouse_code in warehouse_codes:
        __four_bytes_DTF_Account = __warehouse_codes(warehouse_code)
        for row_code in rows_codes:
            base_code = f"U7A7A1758{row_code}"

            if row_code == '0':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                total_part_sales_last_month = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "total_part_sales_last_month"), 
                    0
                )

                total_part_sales_last_month = f"{int(total_part_sales_last_month):09}"

                formatted_string = (
                    f"{base_code}V2{__prev_month}"
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{total_part_sales_last_month}"
                    f"{'0' * 9}{'0' * 9}{'0' * 9}{'0' * 9}"
                    f"{' ' * 6}D1M"
                )

            elif row_code == 'I':
                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 9}{' ' * 42}D1M"
                )
            
            elif row_code == 'J':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                # Extract values from API result (default to 0 if not found)
                avg_inventory_last_12 = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "avg_monthly_inventory_value_last_12_months"), 
                    0
                )
                avg_inventory_prior_12 = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "avg_monthly_inventory_value_prior_12_months"), 
                    0
                )
                total_part_sales_last_12 = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "total_part_sales_last_12_months"), 
                    0
                )
                total_part_sales_prior_12 = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "total_part_sales_prior_12_months"), 
                    0
                )
                total_part_sales_last_month = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "total_part_sales_last_month"), 
                    0
                )

                # Format numbers with leading zeros (9 digits, no decimal)
                avg_inventory_last_12 = f"{int(avg_inventory_last_12):09}"
                avg_inventory_prior_12 = f"{int(avg_inventory_prior_12):09}"
                total_part_sales_last_12 = f"{int(total_part_sales_last_12):09}"
                total_part_sales_prior_12 = f"{int(total_part_sales_prior_12):09}"
                total_part_sales_last_month = f"{int(total_part_sales_last_month):09}"

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{avg_inventory_last_12}"  # 27-35
                    f"{avg_inventory_prior_12}"  # 36-44
                    f"{total_part_sales_last_12}" # 45-53
                    f"{total_part_sales_prior_12}" # 54-62
                    f"{total_part_sales_last_month}" # 63-71
                    f"{' ' * 6}"  # 72-77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'K':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                # Extract values from API result (default to 0 if not found)
                total_parts_cost_of_sales_last_12_months = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "total_parts_cost_of_sales_last_12_months"), 
                    0
                )
                total_parts_cost_of_sales_prior_12_months = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "total_parts_cost_of_sales_prior_12_months"), 
                    0
                )
                total_parts_cost_of_sales_last_month = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "total_parts_cost_of_sales_last_month"), 
                    0
                )
                current_inventory_value = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "current_inventory_value"), 
                    0
                )
                inventory_with_no_sales = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "inventory_with_no_sales"), 
                    0
                )
              
                # Format numbers with leading zeros (9 digits, no decimal)
                total_parts_cost_of_sales_last_12_months = f"{int(total_parts_cost_of_sales_last_12_months):09}"
                total_parts_cost_of_sales_prior_12_months = f"{int(total_parts_cost_of_sales_prior_12_months):09}"
                total_parts_cost_of_sales_last_month = f"{int(total_parts_cost_of_sales_last_month):09}"
                current_inventory_value = f"{int(current_inventory_value):09}"
                inventory_with_no_sales = f"{int(inventory_with_no_sales):09}"

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{total_parts_cost_of_sales_last_12_months}"  # 27-35
                    f"{total_parts_cost_of_sales_prior_12_months}"  # 36-44
                    f"{total_parts_cost_of_sales_last_month}" # 45-53
                    f"{current_inventory_value}" # 54-62
                    f"{inventory_with_no_sales}" # 63-71
                    f"{' ' * 6}"  # 72-77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'L':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                # Extract values from API result (default to 0 if not found)
                total_unique_customers_last_12_months = next(
                    (item["value"] for item in warehouse_data["data"] if item["metric"] == "total_unique_customers_last_12_months"), 
                    0
                )
              
                # Format numbers with leading zeros (9 digits, no decimal)
                total_unique_customers_last_12_months = f"{int(total_unique_customers_last_12_months):05}"

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{total_unique_customers_last_12_months}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{'0' * 5}"  # 37-41
                    f"{'0' * 5}"  # 42-46
                    f"{'0' * 5}"  # 47-51
                    f"{'0' * 5}"  # 52-56
                    f"{'0' * 5}"  # 57-61
                    f"{'0' * 5}"  # 62-66
                    f"{'0' * 5}"  # 67-71
                    f"{'0' * 5}"  # 72-76
                    f"{' '}"  # 77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'M':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 5}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{'0' * 5}"  # 37-41
                    f"{'0' * 5}"  # 42-46
                    f"{'0' * 5}"  # 47-51
                    f"{'0' * 5}"  # 52-56
                    f"{'0' * 5}"  # 57-61
                    f"{'0' * 5}"  # 62-66
                    f"{'0' * 5}"  # 67-71
                    f"{'0' * 5}"  # 72-76
                    f"{' '}"  # 77 (spaces)
                    f"D1M"  # 78-80
                )

            elif row_code == 'N':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 5}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{'0' * 5}"  # 37-41
                    f"{'0' * 5}"  # 42-46
                    f"{'0' * 5}"  # 47-51
                    f"{'0' * 5}"  # 52-56
                    f"{'0' * 5}"  # 57-61
                    f"{'0' * 5}"  # 62-66
                    f"{'0' * 5}"  # 67-71
                    f"{'0' * 5}"  # 72-76
                    f"{' '}"  # 77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'O':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 5}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{'0' * 5}"  # 37-41
                    f"{'0' * 5}"  # 42-46
                    f"{'0' * 5}"  # 47-51
                    f"{'0' * 5}"  # 52-56
                    f"{'0' * 5}"  # 57-61
                    f"{'0' * 5}"  # 62-66
                    f"{'0' * 5}"  # 67-71
                    f"{'0' * 5}"  # 72-76
                    f"{' '}"  # 77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'P':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 5}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{'0' * 5}"  # 37-41
                    f"{'0' * 5}"  # 42-46
                    f"{'0' * 5}"  # 47-51
                    f"{'0' * 5}"  # 52-56
                    f"{'0' * 5}"  # 57-61
                    f"{'0' * 5}"  # 62-66
                    f"{'0' * 5}"  # 67-71
                    f"{'0' * 5}"  # 72-76
                    f"{' '}"  # 77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'Q':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 5}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{'0' * 5}"  # 37-41
                    f"{'0' * 5}"  # 42-46
                    f"{'0' * 5}"  # 47-51
                    f"{'0' * 5}"  # 52-56
                    f"{'0' * 5}"  # 57-61
                    f"{'0' * 5}"  # 62-66
                    f"{'0' * 5}"  # 67-71
                    f"{'0' * 5}"  # 72-76
                    f"{' '}"  # 77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'R':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 5}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{'0' * 5}"  # 37-41
                    f"{'0' * 5}"  # 42-46
                    f"{'0' * 5}"  # 47-51
                    f"{'0' * 5}"  # 52-56
                    f"{'0' * 5}"  # 57-61
                    f"{'0' * 5}"  # 62-66
                    f"{'0' * 5}"  # 67-71
                    f"{'0' * 5}"  # 72-76
                    f"{' '}"  # 77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'S':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 5}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{' ' * 10}"  # 37-46
                    f"{'0' * 9}"  # 47-55
                    f"{' ' * 22}"  # 56-77
                    f"D1M"  # 78-80
                )
            elif row_code == 'T':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 5}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{'0' * 5}"  # 37-41
                    f"{'0' * 5}"  # 42-46
                    f"{'0' * 5}"  # 47-51
                    f"{'0' * 5}"  # 52-56
                    f"{'0' * 5}"  # 57-61
                    f"{'0' * 5}"  # 62-66
                    f"{'0' * 5}"  # 67-71
                    f"{'0' * 5}"  # 72-76
                    f"{' '}"  # 77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'U':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{'0' * 5}"  # 27-31
                    f"{'0' * 5}"  # 32-36
                    f"{' ' * 40}"  # 37-76
                    f"{' '}"  # 77 (spaces)
                    f"D1M"  # 78-80
                )

            __pmmdata.append(formatted_string)
    
    __save_file = save_to_dat_file(__pmmdata)
    if __save_file:
        upload_file_to_api(__save_file, upload_api_url)
    
def save_to_dat_file(results):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    save_directory = os.path.abspath(os.path.join(current_dir, "../../Files/PMM"))
    os.makedirs(save_directory, exist_ok=True)
    today = datetime.date.today()
    formatted_date = today.strftime("%d%b%Y").upper()  # Example: 07MAR2025

    now = datetime.datetime.now()
    formatted_time = now.strftime("%H%M%S")  # Example: 153045
    new_filename = f'DLR2JD_{formatted_date}_{formatted_time}.DAT'

    file_path = os.path.join(save_directory, new_filename)

    try:
        with open(file_path, 'w', encoding='utf-8', newline='\r\n') as file:
            for line in results:
                file.write(line + '\n')

        print(f"File successfully saved at: {file_path}")
        return file_path  # Return file path on success
    except IOError as e:
        print(f"IOError: Failed to save the file. Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    return None  # Return None if saving fails

def upload_file_to_api(filename, api_url):
    upload_token = get_upload_token()
    if not os.path.exists(filename):
        error_message = f"File {filename} not found."
        print(json.dumps({'message': error_message}, indent=4))
        send_email(error_message, 'PMM FILE SENDING FAILED')  # Send failure email
        logging.error(error_message)
        return
    
    headers = {
        'Authorization': f'Bearer {upload_token}'
    }

    with open(filename, 'rb') as file:
        files = {'file': file}
        __email_subject = 'PMM FILE SENDING'
        
        try:
            response = requests.put(api_url, headers=headers, files=files)
            extracted_filename = os.path.basename(filename)
            result = {
                'status_code': response.status_code,
                'filename': extracted_filename
            }

            if response.status_code == 204:
                result['message'] = 'File uploaded successfully'
                body = f"File {extracted_filename} successfully sent."
                send_email(body, __email_subject)
                logging.info(f"File {extracted_filename} uploaded successfully.")
                logging.info(f"{result}")
            else:
                result['message'] = 'Unexpected status code'
                result['response'] = response.text
                error_body = f"File {extracted_filename} upload failed.\nStatus Code: {response.status_code}\nResponse: {response.text}"
                send_email(error_body, 'PMM FILE SENDING FAILED')  # Send failure email
                logging.error(f"Upload failed for {extracted_filename}: {response.text}")
        except requests.exceptions.RequestException as e:
            error_body = f"Error uploading file {filename}.\nError: {str(e)}"
            send_email(error_body, 'PMM FILE SENDING FAILED')  # Send failure email
            logging.error(f"Error uploading {filename}: {str(e)}")
    
    logging.info("Script execution ended successfully.")

def send_email(body, subject):
    # Your Gmail address
    sender_email = os.getenv("PROD_EMAIL_USERNAME")
    # The app password
    app_password = os.getenv("PROD_EMAIL_PASSWORD")

    # Initialize yagmail client
    yag = yagmail.SMTP(sender_email, app_password)

    # Set up the email content
    recipients = [
        'RawalPankaj@johndeere.com',
        'ramajam@abomar.net',
        'abbanay@lmiitsolutions.com',
        'marco@abomar.net'
    ]
    cc=['punzalanpatrickjason@gmail.com','ibayonabel@gmail.com']

    # Set the custom "From" header with your desired display name
    from_name = 'Abomar Notification'
    yag.send(
        to=recipients,
        cc=cc,
        subject=subject,
        contents=body,
        headers={'From': f'{from_name} <{sender_email}>'}
    )

    print('Email sent successfully!')

if __name__ == '__main__':
    main()
