#!/usr/bin/env python3
import xmlrpc.client
import datetime
import json
import logging
import os
import requests
import yagmail
import argparse
from dotenv import load_dotenv

from datetime import timedelta, date, timezone
from dateutil.relativedelta import relativedelta
from collections import defaultdict
from requests.auth import HTTPBasicAuth
import sys

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

# PHP CONVERSION VARIABLES
API_KEY_ENV_VAR = os.getenv("API_KEY_ENV_VAR")
API_URL_ENV_VAR = os.getenv("API_URL_ENV_VAR")
RATE_FROM_CURRENCY = 'PHP'
RATE_TO_CURRENCY = 'USD'
FALLBACK_DATA_FILE = '/var/www/abomar-pmm-api/abomar-pmm/main/pmm/latest_exchange_rate_fallback.json'

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
def get_eligible_product_ids():
    """Fetches eligible product IDs ('JD' AND NOT 'CG')."""
    eligible_ids = set()
    try:
        # Fetch products with default code and their category info
        all_products = models.execute_kw(
            db, uid, password, 'product.product', 'search_read',
            [[('default_code', '!=', False)]],
            {'fields': ['id', 'categ_id']} # Fetch category ID/Name directly
        )

        count_eligible = 0
        for p in all_products:
            categ_info = p.get('categ_id')
            # Basic check: Needs to be a list [id, name]
            if not categ_info or not isinstance(categ_info, list) or len(categ_info) < 2:
                continue

            categ_name = categ_info[1].lower() # Get name (index 1) and lowercase

            # Apply category filters
            if 'john deere' in categ_name and 'complete goods' not in categ_name:
                eligible_ids.add(p['id'])
                count_eligible += 1
        return eligible_ids

    except Exception as e:
        logging.exception("Error fetching eligible product IDs."); return set()

def get_warehouse_mapping():
    """Returns mapping from location group name to warehouse code substring used in sales queries."""
    # Map display names to warehouse codes used in sale order warehouse filter
    return {
        "Cebu": "CEB", "Pasig": "PAS", "Davao City": "DAV",
        "Bacolod": "BAC", "Cagayan de oro": "CAG"
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

# Define the target keywords and their corresponding group names
TARGET_DEFINITIONS = {"CEB": "Cebu", "PAS": "Pasig", "DAV": "Davao City", "BAC": "Bacolod", "CAG": "Cagayan de oro"}
TARGET_KEYWORD_PRIORITY = ["CEB", "PAS", "DAV", "CAG", "BAC"] # Used in valuation filtering

def get_month_end_dates(start_date, end_date):
    """Generate a list of month-end dates between start_date and end_date."""
    dates = []
    current = start_date
    while current <= end_date:
        next_month = current.replace(day=28) + timedelta(days=4)
        month_end = next_month - timedelta(days=next_month.day)
        if month_end > end_date: month_end = end_date
        dates.append(month_end)
        if month_end == end_date: break
        current = month_end + timedelta(days=1)
        if len(dates) > 24: break # Safety break
    return dates

def get_last_month_range():
    """Returns (start, end) dates for the last full calendar month."""
    today = datetime.date.today()
    first_day_current = today.replace(day=1)
    last_day_last_month = first_day_current - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    return (first_day_last_month, last_day_last_month)

def get_actual_last_month_range():
    """Returns (start, end) dates for the actual last full calendar month based on today."""
    today = date.today()
    first_day_current = today.replace(day=1)
    last_day_last_month = first_day_current - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)
    return (first_day_last_month, last_day_last_month)

# ---------------------------
# Inventory Valuation Functions (Aligned with PMMCHECK v9 logic)
# ---------------------------

# Uses separate fetch for complete_name, handles ref_date_str context, prioritized keyword match
def get_inventory_valuation_for_location(target_keyword_to_calculate, eligible_product_ids, ref_date_str=None):
    """
    Calculates inventory value for ELIGIBLE products. Assigns location based on path prefix
    match in complete_name. Aggregates ONLY for target_keyword_to_calculate.
    Uses historical context if ref_date_str is provided, otherwise current.
    Returns: total_value only.
    """
    default_return = 0.0 # Default return value for this function
    if not eligible_product_ids or not target_keyword_to_calculate:
        return default_return

    quant_domain = [
        ('product_id', 'in', list(eligible_product_ids)),
        ('quantity', '!=', 0),
        ('location_id.usage', '=', 'internal')
    ]
    quant_context = {'to_date': ref_date_str} if ref_date_str else {}

    try:
        quants = models.execute_kw(db, uid, password, 'stock.quant', 'search_read',
                                   [quant_domain], {'fields': ['product_id', 'quantity', 'location_id'], 'context': quant_context})
        if not quants: return default_return

        location_ids_in_quants = {q['location_id'][0] for q in quants if q.get('location_id') and isinstance(q['location_id'], list)}
        if not location_ids_in_quants: return default_return

        locations_data = models.execute_kw(db, uid, password, 'stock.location', 'search_read',
                                           [[('id', 'in', list(location_ids_in_quants))]], {'fields': ['id', 'complete_name']})
        location_complete_name_map = {loc['id']: loc.get('complete_name', '') for loc in locations_data}
        if not location_complete_name_map and location_ids_in_quants: return default_return # Safer

        product_qty = defaultdict(float); product_ids_in_target = set()
        unmapped_locations = set() # Local set for this call to avoid excessive logging if needed

        for quant in quants:
            loc_info = quant.get('location_id');
            if not loc_info or not isinstance(loc_info, list) or not loc_info: continue
            loc_id = loc_info[0]; loc_complete_name = location_complete_name_map.get(loc_id)
            if not loc_complete_name: continue

            # Determine target based on first path component
            quant_target = None
            parts = loc_complete_name.split('/')
            if parts:
                first_part_upper = parts[0].strip().upper()
                if first_part_upper in TARGET_DEFINITIONS: # Check against keys CEB, PAS etc.
                    quant_target = first_part_upper

            if quant_target is None:
                # if loc_complete_name not in unmapped_locations:
                #    logging.debug(f"Location '{loc_complete_name}' (ID: {loc_id}) did not map to a target prefix.")
                #    unmapped_locations.add(loc_complete_name)
                continue

            # Only aggregate if matches the target we are currently calculating
            if quant_target == target_keyword_to_calculate.upper():
                prod_info = quant.get('product_id'); qty = quant.get('quantity', 0.0)
                if not prod_info or qty == 0: continue
                prod_id = prod_info[0] if isinstance(prod_info, list) else prod_info
                if prod_id: product_qty[prod_id] += qty; product_ids_in_target.add(prod_id)

        if not product_ids_in_target:
            return default_return

        cost_context = {'to_date': ref_date_str} if ref_date_str else {} # Use context for cost if needed
        products = models.execute_kw(db, uid, password, 'product.product', 'search_read',
                                     [[('id', 'in', list(product_ids_in_target))]],
                                     {'fields': ['id', 'avg_cost'], 'context': cost_context})
        if not products:
             logging.warning(f"Could not fetch cost details (context {ref_date_str}) for products: {list(product_ids_in_target)}"); return default_return

        cost_map = {prod['id']: prod.get('avg_cost', 0.0) or 0.0 for prod in products}
        total_value = sum(qty * cost_map.get(prod_id, 0.0) for prod_id, qty in product_qty.items())
        return total_value # Return only the value

    except Exception as e:
        logging.exception(f"Error getting inv value target {target_keyword_to_calculate} context {ref_date_str}"); return default_return


# MODIFIED: Calls new valuation function
def average_inventory_valuation(target_keyword, eligible_product_ids, month_end_dates):
    """Compute the average inventory valuation across given month-end dates using historical costs."""
    total = 0.0; count = 0
    for dt in month_end_dates:
        ref_str = dt.strftime("%Y-%m-%d 23:59:59")
        # Pass target_keyword and ref_date for historical cost & stock snapshot
        val = get_inventory_valuation_for_location(target_keyword, eligible_product_ids, ref_str)
        total += val; count += 1
    return (total / count) if count > 0 else 0.0

# ---------------------------
# Revised Sales Functions (Aligned with PMMCHECK)
# ---------------------------
def get_total_sales_for_location_eligible_products(warehouse_code, start_date, end_date, eligible_product_ids):
    """Sums 'price_subtotal' from sale lines filtered by eligible products and warehouse."""
    if not eligible_product_ids or not warehouse_code: return 0.0
    sol_domain = [
        ('order_id.state', 'in', ['sale', 'done']),
        ('order_id.date_order', '>=', start_date.strftime('%Y-%m-%d')),
        ('order_id.date_order', '<=', end_date.strftime('%Y-%m-%d')),
        ('order_id.warehouse_id.name', 'ilike', warehouse_code),
        ('product_id', 'in', list(eligible_product_ids))
    ]
    try:
        lines = models.execute_kw(db, uid, password, 'sale.order.line', 'search_read',
                                  [sol_domain], {'fields': ['price_subtotal']})
        return sum(line.get('price_subtotal', 0.0) for line in lines)
    except Exception as e:
        logging.exception(f"Error getting total sales for WH {warehouse_code}"); return 0.0

# RENAMED function, aligned logic with PMMCHECK
def get_total_sale_lines_for_location_eligible_products(warehouse_code, start_date, end_date, eligible_product_ids):
    """Counts sale order lines filtered by eligible products and warehouse."""
    if not eligible_product_ids or not warehouse_code: return 0
    sol_domain = [
        ('order_id.state', 'in', ['sale', 'done']),
        ('order_id.date_order', '>=', start_date.strftime('%Y-%m-%d')),
        ('order_id.date_order', '<=', end_date.strftime('%Y-%m-%d')),
        ('order_id.warehouse_id.name', 'ilike', warehouse_code),
        ('product_id', 'in', list(eligible_product_ids))
    ]
    try:
        return models.execute_kw(db, uid, password, 'sale.order.line', 'search_count', [sol_domain])
    except Exception as e:
        logging.exception(f"Error getting sale line count for WH {warehouse_code}"); return 0

# ---------------------------
# Revised Parts Cost of Sales Function (COS) (Aligned Filter, Historical Context)
# ---------------------------
def get_parts_cost_of_sales_for_location(warehouse_code, start_date, end_date, eligible_product_ids, ref_date_str):
    """
    Calculate COS for ELIGIBLE products using avg_cost as of ref_date_str.
    """
    if not eligible_product_ids or not warehouse_code: return 0.0
    sol_domain = [
        ('order_id.state', 'in', ['sale', 'done']),
        ('order_id.date_order', '>=', start_date.strftime('%Y-%m-%d')),
        ('order_id.date_order', '<=', end_date.strftime('%Y-%m-%d')),
        ('order_id.warehouse_id.name', 'ilike', warehouse_code),
        ('product_id', 'in', list(eligible_product_ids)) # Use eligible IDs
    ]
    try:
        sale_lines = models.execute_kw(db, uid, password, 'sale.order.line', 'search_read',
                                       [sol_domain], {'fields': ['product_id', 'product_uom_qty']})
        if not sale_lines: return 0.0
        product_qty_sold = defaultdict(float); product_ids_sold = set()
        for line in sale_lines:
            product_info = line.get('product_id'); qty = line.get('product_uom_qty', 0.0)
            if not product_info or qty <= 0: continue
            prod_id = product_info[0] if isinstance(product_info, list) else product_info
            product_qty_sold[prod_id] += qty; product_ids_sold.add(prod_id)
        if not product_ids_sold: return 0.0

        cost_context = {'to_date': ref_date_str} # Keep context for PMM historical
        products_costs = models.execute_kw(db, uid, password, 'product.product', 'search_read',
                                           [[('id', 'in', list(product_ids_sold))]],
                                           {'fields': ['id', 'avg_cost'], 'context': cost_context})
        if not products_costs: logging.warning(f"Could not get costs context {ref_date_str} for {list(product_ids_sold)}"); return 0.0
        cost_map = {prod['id']: prod.get('avg_cost', 0.0) or 0.0 for prod in products_costs}
        return sum(sold_qty * cost_map.get(prod_id, 0.0) for prod_id, sold_qty in product_qty_sold.items())
    except Exception as e:
        logging.exception(f"Error getting COS WH {warehouse_code} context {ref_date_str}"); return 0.0

# ---------------------------
# Inventory with No Sales Function (MODIFIED for Alignment)
# ---------------------------
def get_inventory_with_no_sales_for_location(target_keyword, warehouse_code, sales_start, sales_end, eligible_product_ids, ref_date_str):
    """
    Computes inventory value (as of ref_date_str) for ELIGIBLE products on hand in locations matching target_keyword,
    created > 12 months before ref_date_str, with no sales from warehouse_code during sales period.
    """
    if not eligible_product_ids or not target_keyword or not warehouse_code:
        logging.warning("Inv No Sales skipped: Missing args."); return 0.0

    # 1. Find eligible products on hand in the target locations AS OF ref_date_str
    quant_domain = [
        ('product_id', 'in', list(eligible_product_ids)),
        ('quantity', '>', 0),
        ('location_id.usage', '=', 'internal')
    ]
    quant_context = {'to_date': ref_date_str}
    try:
        quants = models.execute_kw(db, uid, password, 'stock.quant', 'search_read',
                                   [quant_domain], {'fields': ['product_id', 'quantity', 'location_id'], 'context': quant_context})
        if not quants: logging.info(f"Inv No Sales: No quants found for context {ref_date_str}."); return 0.0

        location_ids_in_quants = {q['location_id'][0] for q in quants if q.get('location_id') and isinstance(q['location_id'], list)}
        if not location_ids_in_quants: logging.warning("Inv No Sales: No valid location IDs in quants."); return 0.0

        locations_data = models.execute_kw(db, uid, password, 'stock.location', 'search_read',
                                           [[('id', 'in', list(location_ids_in_quants))]], {'fields': ['id', 'complete_name']})
        location_complete_name_map = {loc['id']: loc.get('complete_name', '') for loc in locations_data}

        hist_on_hand_qty = defaultdict(float)
        on_hand_product_ids_in_target = set()
        for quant in quants:
            loc_info = quant.get('location_id');
            if not loc_info or not isinstance(loc_info, list) or not loc_info: continue
            loc_id = loc_info[0]; loc_complete_name = location_complete_name_map.get(loc_id)
            if not loc_complete_name: continue

            # Determine definitive target based on priority
            quant_target = None; loc_complete_name_upper = loc_complete_name.upper()
            for keyword in TARGET_KEYWORD_PRIORITY:
                if keyword in loc_complete_name_upper: quant_target = keyword; break

            if quant_target == target_keyword.upper():
                prod_info = quant.get('product_id'); qty = quant.get('quantity', 0.0)
                if not prod_info or qty <= 0: continue # Use <= 0 to be safe
                prod_id = prod_info[0] if isinstance(prod_info, list) else prod_info
                if prod_id: hist_on_hand_qty[prod_id] += qty; on_hand_product_ids_in_target.add(prod_id)

        if not on_hand_product_ids_in_target: logging.info(f"Inv No Sales: No on-hand products found matching target '{target_keyword}' context {ref_date_str}."); return 0.0

        # 2. Filter by age
        threshold_date = datetime.datetime.strptime(ref_date_str, "%Y-%m-%d %H:%M:%S") - relativedelta(years=1)
        templates_aged = models.execute_kw(db, uid, password, 'product.template', 'search_read',
                                           [[('create_date', '<=', threshold_date.strftime("%Y-%m-%d %H:%M:%S"))]], {'fields': ['id']})
        template_ids_aged = {tmpl['id'] for tmpl in templates_aged}
        products_aged = models.execute_kw(db, uid, password, 'product.product', 'search_read',
                                          [[('id', 'in', list(on_hand_product_ids_in_target)), ('product_tmpl_id', 'in', list(template_ids_aged))]], {'fields': ['id']})
        aged_on_hand_product_ids = {prod['id'] for prod in products_aged}
        if not aged_on_hand_product_ids: logging.info("Inv No Sales: No aged products among those on hand."); return 0.0

        # 3. Find sold products
        sold_domain = [
            ('order_id.state', 'in', ['sale', 'done']),
            ('order_id.date_order', '>=', sales_start.strftime('%Y-%m-%d')),
            ('order_id.date_order', '<=', sales_end.strftime('%Y-%m-%d')),
            ('order_id.warehouse_id.name', 'ilike', warehouse_code),
            ('product_id', 'in', list(aged_on_hand_product_ids))
        ]
        sold_lines = models.execute_kw(db, uid, password, 'sale.order.line', 'read_group',
                                       [sold_domain], {'fields': ['product_id'], 'groupby': ['product_id']}, {'lazy': False})
        sold_product_ids_in_period = {line['product_id'][0] for line in sold_lines if line.get('product_id')}

        # 4. Identify no-sales products
        no_sales_product_ids = aged_on_hand_product_ids - sold_product_ids_in_period
        if not no_sales_product_ids: logging.info("Inv No Sales: All aged on-hand products had sales."); return 0.0

        # 5. Get historical cost and calculate value
        cost_context = {'to_date': ref_date_str}
        products_costs = models.execute_kw(db, uid, password, 'product.product', 'search_read',
                                           [[('id', 'in', list(no_sales_product_ids))]], {'fields': ['id', 'avg_cost'], 'context': cost_context})
        if not products_costs: logging.warning(f"Inv No Sales: Could not get costs for {list(no_sales_product_ids)}"); return 0.0

        cost_map = {prod['id']: prod.get('avg_cost', 0.0) or 0.0 for prod in products_costs}
        # Use the historical on-hand quantity calculated in step 1
        total_value_no_sales = sum(hist_on_hand_qty.get(prod_id, 0.0) * cost_map.get(prod_id, 0.0) for prod_id in no_sales_product_ids)
        return total_value_no_sales
    except Exception as e:
        logging.exception(f"Error getting Inv No Sales for target {target_keyword}"); return 0.0

# ---------------------------
# Main Reporting Function
# ---------------------------
def main():
    logging.info("PMM Main function started.")

    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Generate PMM report, optionally anchored to a specific historical month.")
    parser.add_argument("--year", type=int, help="Target year for the 'Last Month' anchor (e.g., 2024)")
    parser.add_argument("--month", type=int, help="Target month for the 'Last Month' anchor (1-12)")
    args = parser.parse_args()

    today = date.today()
    if args.year and args.month:
        try:
            if not (1 <= args.month <= 12):
                raise ValueError("Month must be between 1 and 12.")
            # Calculate start and end for the specified month
            target_month_start = date(args.year, args.month, 1)
            if args.month == 12:
                target_month_end = date(args.year, args.month, 31)
            else:
                target_month_end = date(args.year, args.month + 1, 1) - timedelta(days=1)

            # Check if specified month is valid (not in the future relative to today)
            if target_month_end >= today:
                 print(f"Error: Specified month {args.year}-{args.month:02d} has not fully completed. Please specify a past month or run without arguments for default behavior.")
                 logging.error(f"Specified month {args.year}-{args.month:02d} is not a completed past month.")
                 sys.exit(1)

            # Assign the target month dates as the "last month" for calculation purposes
            last_month_start = target_month_start
            last_month_end = target_month_end
            report_period_label = f"Report Based on Specified Month: {last_month_end.strftime('%B %Y')}"
            logging.info(f"Using specified anchor month: {last_month_start.strftime('%Y-%m-%d')} to {last_month_end.strftime('%Y-%m-%d')}")

        except ValueError as e:
            print(f"Error: Invalid year or month provided ({args.year}-{args.month}). {e}")
            logging.error(f"Invalid year or month provided: {args.year}-{args.month}. {e}")
            sys.exit(1)
    else:
        # Default: Use the actual last completed month
        last_month_start, last_month_end = get_actual_last_month_range()
        report_period_label = f"Report Based on Last Complete Month: {last_month_end.strftime('%B %Y')}"
        logging.info(f"Using default anchor month (last complete month): {last_month_start.strftime('%Y-%m-%d')} to {last_month_end.strftime('%Y-%m-%d')}")

    eligible_product_ids = get_eligible_product_ids()
    if not eligible_product_ids:
        print("No eligible products found based on category criteria. Cannot generate report.")
        logging.error("No eligible products found based on category criteria."); return
    wh_mapping = get_warehouse_mapping()     # mapping: group -> warehouse code substring

     # --- Calculate Dependent Date Ranges ---
    try:
        # These are now relative to the determined last_month_start/end
        last12_start = last_month_start - relativedelta(years=1)
        last12_end = last_month_end
        prior12_start = last12_start - relativedelta(years=1)
        prior12_end = last12_start - timedelta(days=1)
        logging.info(f"Derived Date Ranges: Last12({last12_start} to {last12_end}), Prior12({prior12_start} to {prior12_end})")

        # Generate month-end dates for the periods.
        last12_month_ends = get_month_end_dates(last12_start, last12_end)
        prior12_month_ends = get_month_end_dates(prior12_start, prior12_end)
        logging.info(f"Generated {len(last12_month_ends)} month-ends for Last12, {len(prior12_month_ends)} for Prior12.")
        if not last12_month_ends or not prior12_month_ends:
             raise ValueError("Failed to generate month-end dates lists.")

        # Define the reference date for snapshot calculations based on the determined "last month"
        ref_date_str_hist = last_month_end.strftime("%Y-%m-%d 23:59:59")
        logging.info(f"Using historical reference date for snapshot values: {ref_date_str_hist}")

    except Exception as e:
        logging.exception("Error calculating dependent date ranges."); print("Error calculating dependent date ranges."); return
    # eligible_product_ids = get_eligible_product_ids()
    # if not eligible_product_ids:
    #     print("No eligible products found based on category criteria. Cannot generate report.")
    #     logging.error("No eligible products found based on category criteria."); return
    # wh_mapping = get_warehouse_mapping()     # mapping: group -> warehouse code substring

    # # Define date ranges
    # try:
    #     last_month_start, last_month_end = get_last_month_range()
    #     last12_start = last_month_start - relativedelta(years=1)
    #     last12_end = last_month_end
    #     prior12_start = last12_start - relativedelta(years=1)
    #     prior12_end = last12_start - timedelta(days=1)
    #     logging.info(f"Date Ranges: LastMonth({last_month_start} to {last_month_end}), Last12({last12_start} to {last12_end}), Prior12({prior12_start} to {prior12_end})")
    # except Exception as e:
    #     logging.exception("Error calculating date ranges."); print("Error calculating date ranges."); return


    # ref_date_str = last_month_end.strftime("%Y-%m-%d 23:59:59")
    
    # # Generate month-end dates for the periods.
    # last12_month_ends = get_month_end_dates(last12_start, last12_end)
    # prior12_month_ends = get_month_end_dates(prior12_start, prior12_end)
    # ref_date_str_hist = last_month_end.strftime("%Y-%m-%d 23:59:59")
    results = []

    for target_keyword, group_name in TARGET_DEFINITIONS.items():
        wh_code = wh_mapping.get(group_name, "")
        print(wh_code)
        group_data = {
            "location": group_name,
            "warehouse_code": wh_code,
            "data": [
                {"metric": "avg_monthly_inventory_value_last_12_months", "value": average_inventory_valuation(target_keyword, eligible_product_ids, last12_month_ends)},
                {"metric": "avg_monthly_inventory_value_prior_12_months", "value": average_inventory_valuation(target_keyword, eligible_product_ids, prior12_month_ends)},
                {"metric": "total_part_sales_last_12_months", "value": get_total_sales_for_location_eligible_products(wh_code, last12_start, last12_end, eligible_product_ids)},
                {"metric": "total_part_sales_prior_12_months", "value": get_total_sales_for_location_eligible_products(wh_code, prior12_start, prior12_end, eligible_product_ids)},
                {"metric": "total_part_sales_last_month", "value": get_total_sales_for_location_eligible_products(wh_code, last_month_start, last_month_end, eligible_product_ids)},
                {"metric": "total_parts_cost_of_sales_last_12_months", "value": get_parts_cost_of_sales_for_location(wh_code, last12_start, last12_end, eligible_product_ids, ref_date_str_hist)},
                {"metric": "total_parts_cost_of_sales_prior_12_months", "value": get_parts_cost_of_sales_for_location(wh_code, prior12_start, prior12_end, eligible_product_ids, ref_date_str_hist)},
                {"metric": "total_parts_cost_of_sales_last_month", "value": get_parts_cost_of_sales_for_location(wh_code, last_month_start, last_month_end, eligible_product_ids, ref_date_str_hist)},
                {"metric": "current_inventory_value", "value": get_inventory_valuation_for_location(target_keyword, eligible_product_ids, ref_date_str_hist)},
                {"metric": "inventory_with_no_sales", "value": get_inventory_with_no_sales_for_location(target_keyword, wh_code, last12_start, last12_end, eligible_product_ids, ref_date_str_hist)},
                {"metric": "total_unique_customers_last_12_months", "value": get_total_sale_lines_for_location_eligible_products(wh_code, last_month_start, last_month_end, eligible_product_ids)}
            ]
        }

        results.append(group_data)

    pmm_data_result = results
    # print(json.dumps(results, indent=4))

    warehouse_codes = ['CEB', 'PAS', 'DAV', 'BAC', 'CAG']
    rows_codes = ['0', 'I', 'J', 'K', 'L','M', 'N', 'O', 'P', 'Q','R', 'S', 'T', 'U']
    # __prev_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y%m")
    __year_month = last_month_start.strftime("%Y%m")

    __pmmdata = []
    __four_bytes_DTF_Account = ''
    for warehouse_code in warehouse_codes:
        __four_bytes_DTF_Account = __warehouse_codes(warehouse_code)
        for row_code in rows_codes:
            base_code = f"U7A7A1758{row_code}"

            if row_code == '0':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)

                total_part_sales_last_month_usd = get_and_convert_metric(warehouse_data, "total_part_sales_last_month")
                __formatted_total_part_sales_last_month = f"{int(total_part_sales_last_month_usd):09}"

                formatted_string = (
                    f"{base_code}V2{__year_month}"
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{__formatted_total_part_sales_last_month}"
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

                avg_inventory_last_12_usd = get_and_convert_metric(warehouse_data, "avg_monthly_inventory_value_last_12_months")
                avg_inventory_prior_12_usd = get_and_convert_metric(warehouse_data, "avg_monthly_inventory_value_prior_12_months")
                total_part_sales_last_12_usd = get_and_convert_metric(warehouse_data, "total_part_sales_last_12_months")
                total_part_sales_prior_12_usd = get_and_convert_metric(warehouse_data, "total_part_sales_prior_12_months")
                total_part_sales_last_month_usd = get_and_convert_metric(warehouse_data, "total_part_sales_last_month")

                # Format numbers with leading zeros (9 digits, no decimal)
                __formatted_avg_inventory_last_12 = f"{int(avg_inventory_last_12_usd):09}"
                __formatted_avg_inventory_prior_12 = f"{int(avg_inventory_prior_12_usd):09}"
                __formatted_total_part_sales_last_12 = f"{int(total_part_sales_last_12_usd):09}"
                __formatted_total_part_sales_prior_12 = f"{int(total_part_sales_prior_12_usd):09}"
                __formatted_total_part_sales_last_month = f"{int(total_part_sales_last_month_usd):09}"

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{__formatted_avg_inventory_last_12}"  # 27-35
                    f"{__formatted_avg_inventory_prior_12}"  # 36-44
                    f"{__formatted_total_part_sales_last_12}" # 45-53
                    f"{__formatted_total_part_sales_prior_12}" # 54-62
                    f"{__formatted_total_part_sales_last_month}" # 63-71
                    f"{' ' * 6}"  # 72-77 (spaces)
                    f"D1M"  # 78-80
                )
            elif row_code == 'K':
                # Find matching warehouse data
                warehouse_data = next((w for w in pmm_data_result if w["warehouse_code"] == warehouse_code), None)
              
                total_parts_cost_of_sales_last_12_months_usd = get_and_convert_metric(warehouse_data, "total_parts_cost_of_sales_last_12_months")
                total_parts_cost_of_sales_prior_12_months_usd = get_and_convert_metric(warehouse_data, "total_parts_cost_of_sales_prior_12_months")
                total_parts_cost_of_sales_last_month_usd = get_and_convert_metric(warehouse_data, "total_parts_cost_of_sales_last_month")
                current_inventory_value_usd = get_and_convert_metric(warehouse_data, "current_inventory_value")
                inventory_with_no_sales_usd = get_and_convert_metric(warehouse_data, "inventory_with_no_sales")
              
                # Format numbers with leading zeros (9 digits, no decimal)
                __formatted_total_parts_cost_of_sales_last_12_months = f"{int(total_parts_cost_of_sales_last_12_months_usd):09}"
                __formatted_total_parts_cost_of_sales_prior_12_months = f"{int(total_parts_cost_of_sales_prior_12_months_usd):09}"
                __formatted_total_parts_cost_of_sales_last_month = f"{int(total_parts_cost_of_sales_last_month_usd):09}"
                __formatted_current_inventory_value = f"{int(current_inventory_value_usd):09}"
                __formatted_inventory_with_no_sales = f"{int(inventory_with_no_sales_usd):09}"

                formatted_string = (
                    f"{base_code}{' ' * 2}{' ' * 6}"  # Spaces at 11-12 and 13-18
                    f"{' ' * 3}P{__four_bytes_DTF_Account}"
                    f"{__formatted_total_parts_cost_of_sales_last_12_months}"  # 27-35
                    f"{__formatted_total_parts_cost_of_sales_prior_12_months}"  # 36-44
                    f"{__formatted_total_parts_cost_of_sales_last_month}" # 45-53
                    f"{__formatted_current_inventory_value}" # 54-62
                    f"{__formatted_inventory_with_no_sales}" # 63-71
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

def load_latest_fallback_rate(filepath):
    if not os.path.exists(filepath):
        logging.info(f"Latest fallback file not found: {filepath}")
        return None

    try:
        with open(filepath, 'r') as f:
            content = f.read().strip()
            if not content:
                logging.info(f"Latest fallback file {filepath} is empty.")
                return None

            data = json.loads(content)

            rate_key = f'rate_{RATE_FROM_CURRENCY.lower()}_to_{RATE_TO_CURRENCY.lower()}'
            if isinstance(data, dict) and rate_key in data and 'timestamp' in data:
                logging.info(f"Successfully loaded latest fallback data from {filepath} (recorded on {data['timestamp']}).")
                return data
            else:
                logging.warning(f"Latest fallback file {filepath} has invalid format (expected dictionary with '{rate_key}' and 'timestamp').")
                return None

    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error loading or decoding latest fallback file {filepath}: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading latest fallback data: {e}")
        return None
def save_latest_fallback_rate(filepath, rate_value):
    latest_entry = {
        f'rate_{RATE_FROM_CURRENCY.lower()}_to_{RATE_TO_CURRENCY.lower()}': rate_value,
        'timestamp': datetime.datetime.now(timezone.utc).isoformat()
    }

    try:
        with open(filepath, 'w') as f:
            json.dump(latest_entry, f, indent=4) # Use indent for readability
        logging.info(f"Successfully saved latest rate ({rate_value:.6f}) to fallback file {filepath}.")
    except IOError as e:
        logging.error(f"Error writing to latest fallback file {filepath}: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving latest fallback rate: {e}")

def fetch_exchange_rate_php_to_usd():
    api_key = API_KEY_ENV_VAR
    rate_value = None # This will store USD per 1 PHP
    source = None
    rate_timestamp = None # Timestamp of the rate we end up using
    api_error_details = None
    rate_key = f'rate_{RATE_FROM_CURRENCY.lower()}_to_{RATE_TO_CURRENCY.lower()}'

    logging.info(f"Attempting to fetch {RATE_FROM_CURRENCY} to {RATE_TO_CURRENCY} exchange rate...")

    # --- Attempt API Call to get the rate for 1 unit ---
    if api_key:
        url = API_URL_ENV_VAR
        params = {
            'from': RATE_FROM_CURRENCY,
            'to': RATE_TO_CURRENCY,
            'amount': 1, # Request conversion of 1 unit to get the direct rate
            'access_key': api_key
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get('success') is True and 'result' in data and data['result'] is not None:
                rate_value = float(data['result'])
                source = 'API'
                save_latest_fallback_rate(FALLBACK_DATA_FILE, rate_value)
                rate_timestamp = datetime.datetime.now(timezone.utc).isoformat()

                inverse_rate_php_per_usd = None
                if rate_value is not None and rate_value != 0:
                     try:
                         inverse_rate_php_per_usd = 1 / rate_value
                         logging.info(
                            f"CONVERSION_RATE_FETCHED: 1 {RATE_TO_CURRENCY} = {inverse_rate_php_per_usd:.6f} {RATE_FROM_CURRENCY}, " # Log 1 USD = X PHP
                            f"SOURCE: {source}, TIMESTAMP: {rate_timestamp}"
                        )
                     except ZeroDivisionError:
                         logging.error(f"API returned a rate of 0 {RATE_TO_CURRENCY} per 1 {RATE_FROM_CURRENCY}. Cannot calculate inverse rate.")
                         logging.info(
                            f"CONVERSION_RATE_FETCHED: 1 {RATE_FROM_CURRENCY} = {rate_value:.6f} {RATE_TO_CURRENCY}, "
                            f"SOURCE: {source}, TIMESTAMP: {rate_timestamp}"
                        )

            elif data.get('success') is False and 'error' in data:
                 api_error_details = data.get('error', {})
                 logging.warning(f"API Failed to fetch rate: Code={api_error_details.get('code')}, Type={api_error_details.get('type')}, Info={api_error_details.get('info')}")

            else:
                 logging.warning("API Failed to fetch rate: Unexpected API response format.")
                 logging.debug(f"Unexpected API response: {data}")
                 api_error_details = {'type': 'unexpected_format', 'info': 'API response did not contain expected success/error structure for rate fetch'}

        except requests.exceptions.RequestException as e:
            logging.warning(f"Network or API request error occurred while fetching rate: {e}")
            api_error_details = {'type': type(e).__name__, 'info': str(e)}
        except Exception as e:
            logging.error(f"An unexpected error occurred while fetching rate: {e}")
            api_error_details = {'type': type(e).__name__, 'info': str(e)}

    else:
         logging.warning(f"API key not found ({API_KEY_ENV_VAR} environment variable not set).")
         api_error_details = {'type': 'missing_api_key', 'info': f'{API_KEY_ENV_VAR} environment variable not set'}


    # --- Attempt Fallback if API Failed or No Key AND rate_value is still None ---
    if rate_value is None:
        logging.info("API rate fetch failed or skipped, attempting to use latest fallback data...")
        latest_fallback_entry = load_latest_fallback_rate(FALLBACK_DATA_FILE)

        if latest_fallback_entry:
            rate_value = latest_fallback_entry.get(rate_key)
            rate_timestamp = latest_fallback_entry.get('timestamp')
            source = 'Fallback'
            if rate_value is not None and rate_timestamp is not None:
                # --- LOGGING THE OBTAINED RATE IN REVERSED FORMAT (FROM FALLBACK) ---
                inverse_rate_php_per_usd = None
                if rate_value != 0:
                     try:
                         inverse_rate_php_per_usd = 1 / rate_value
                         logging.info(
                            f"CONVERSION_RATE_FETCHED: 1 {RATE_TO_CURRENCY} = {inverse_rate_php_per_usd:.6f} {RATE_FROM_CURRENCY}, " # Log 1 USD = X PHP
                            f"SOURCE: {source}, TIMESTAMP: {rate_timestamp}"
                         )
                     except ZeroDivisionError:
                         logging.error(f"Fallback rate is 0 {RATE_TO_CURRENCY} per 1 {RATE_FROM_CURRENCY}. Cannot calculate inverse rate.")
                         logging.info( # Still log the rate as obtained
                            f"CONVERSION_RATE_FETCHED: 1 {RATE_FROM_CURRENCY} = {rate_value:.6f} {RATE_TO_CURRENCY}, "
                            f"SOURCE: {source}, TIMESTAMP: {rate_timestamp}"
                         )

            else:
                 logging.error(f"Fallback data loaded but missing required keys ('{rate_key}' or 'timestamp'). Fallback unusable.")
                 source = None
                 rate_timestamp = None
                 # rate_value remains None

        else:
            logging.warning("No latest fallback data available. Cannot obtain exchange rate.")

    if rate_value is None:
         logging.critical(f"RATE_FETCH_FATAL: Could not obtain exchange rate from API or fallback after all attempts.")
         if api_attempted := bool(api_key):
              logging.debug(f"API Attempted: {api_attempted}, API Error Details: {api_error_details}")
         logging.debug(f"Fallback File Path: {FALLBACK_DATA_FILE}, Exists: {os.path.exists(FALLBACK_DATA_FILE)}")

    return rate_value, source, rate_timestamp

# --- Module-level variables to store the fetched rate ---
# Initialize with None
_CURRENT_EXCHANGE_RATE_PHP_TO_USD = None
_RATE_SOURCE = None
_RATE_TIMESTAMP = None # Timestamp associated with the obtained rate

def perform_php_to_usd_conversion(amount):
    # Ensure amount is treated as a number
    try:
        amount = float(amount)
    except (ValueError, TypeError):
        logging.error(f"Invalid amount for conversion: '{amount}'. Must be a number.")
        return None

    # Check if the rate was successfully fetched and stored
    if _CURRENT_EXCHANGE_RATE_PHP_TO_USD is None:
        logging.error(f"Exchange rate not available. Cannot convert {amount:.2f} PHP.")
        # The initial rate fetch log should explain why the rate is None
        return None

    # Perform the conversion
    converted_amount = amount * _CURRENT_EXCHANGE_RATE_PHP_TO_USD

    return converted_amount

def get_and_convert_metric(warehouse_data, metric_name):
    # 1. Extract value, defaulting to 0 if metric is missing or value is not numeric
    php_value = next(
        (item.get("value", 0) for item in warehouse_data.get("data", []) if item.get("metric") == metric_name),
        0 # Default to 0 if the metric_name is not found in the data list
    )

    try:
        php_value = float(php_value)
    except (ValueError, TypeError):
        logging.warning(f"Metric '{metric_name}' has non-numeric value '{php_value}'. Using 0.0 for conversion.")
        php_value = 0.0 # Default to 0.0 if the extracted value is not numeric

    # 2. Perform conversion using the module-level stored rate
    # This function now directly uses the stored rate and handles logging
    usd_value = perform_php_to_usd_conversion(php_value)

    # 3. Handle case where conversion couldn't be performed (rate was None)
    if usd_value is None:
        # perform_php_to_usd_conversion already logged an error if rate was unavailable.
        # We need a default value for the fixed-width formatting that follows.
        # Log a specific warning here that this metric's conversion failed.
        logging.warning(f"Conversion failed for metric '{metric_name}' (PHP value {php_value}) due to unavailable rate. Formatting as 0.")
        return 0.0 # Return 0.0 as default if conversion ultimately failed
    else:
        return usd_value # Return the successfully converted USD value
    
if __name__ == '__main__':
    _CURRENT_EXCHANGE_RATE_PHP_TO_USD, _RATE_SOURCE, _RATE_TIMESTAMP = fetch_exchange_rate_php_to_usd()

    # --- Check if we successfully got a rate ---
    if _CURRENT_EXCHANGE_RATE_PHP_TO_USD is None:
        logging.critical("\nFATAL ERROR: Could not obtain exchange rate from API or fallback. Exiting.")
        exit(1) # Exit the script if we don't have a rate

    main()
