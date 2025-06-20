from functions import PROCESS_DAT_PRICE_FILE, get_model_fields, GET_PARTNERS, GET_PRICELISTS, GET_CATEGORY,GET_PRODUCT_TMPL_ID
import json
import logging

#get_model_fields, PROCESS_DAT_PRICE_FILE, UPDATE_VENDOR_PRICELIST, GET_PRODUCT_TMPL_ID, GET_CURRENCY, GET_PRODUCT
import os
def folder_has_files(folder_path):
    return any(os.path.isfile(os.path.join(folder_path, file)) for file in os.listdir(folder_path))

FULL_DECOMPRESSED_FILES = "/var/www/abomar-pmm-api/abomar-pmm/Files/Decompressed_Files/FULL"
NET_DECOMPRESSED_FILES = "/var/www/abomar-pmm-api/abomar-pmm/Files/Decompressed_Files/NET"

log_filename = "/var/www/abomar-pmm-api/abomar-pmm/main/odoo/PRICEFILE.log"  # Constant log file name
logging.basicConfig(
    filename=log_filename,
    filemode="a",  # Append mode: new log entries are added to the existing file
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.info("Script execution started.")

if folder_has_files(FULL_DECOMPRESSED_FILES):
    print("FULL PRICEFILE FOUND, Processing....")
    PROCESS_FULL = PROCESS_DAT_PRICE_FILE(FULL_DECOMPRESSED_FILES, 'FULL_FILE_BACKUP', 'FULL')
    logging.info(f"{PROCESS_FULL}")
    logging.info("Script execution ended successfully.")
else:
    print("No files found in FULL_DECOMPRESSED_FILES.")
    logging.info(f"No files found in FULL DECOMPRESSED FILES.")
    logging.info("Script execution ended successfully.")

if folder_has_files(NET_DECOMPRESSED_FILES):
    PROCESS_NET = PROCESS_DAT_PRICE_FILE(NET_DECOMPRESSED_FILES, 'NET_FILE_BACKUP', 'NET')
    logging.info(f"{PROCESS_NET}")
    logging.info("Script execution ended successfully.")
else:
    logging.info(f"No files found in NET DECOMPRESSED FILES.")
    logging.info("Script execution ended successfully.")
    print("No files found in NET_DECOMPRESSED_FILES.")

# def START_DATE_CHECKER(directory, type, batch_size=20000, retry=5):
#     """
#     Process DAT price file in batches, retrying if rate limits are encountered.
#     """
#     # Initialize counters for records with and without start_date
#     records_with_start_date = 0
#     records_without_start_date = 0
#     print(f"Start date checker, Processing....")
#     try:
#         for filename in os.listdir(directory):
#             if filename.endswith(".DAT"):
#                 file_path = os.path.join(directory, filename)
#                 with open(file_path, 'r') as dat_file:
#                     header = next(dat_file)  # Skip header
#                     effective_date = header[54:62]

#                     for line in dat_file:
#                         start_date = line[208:216].strip()

#                         # Check if start_date is present
#                         if start_date:
#                             records_with_start_date += 1
#                         else:
#                             records_without_start_date += 1
#                             # You can log or process further if needed for records without start_date

#         # Print the results
#         print(f"Records with start_date: {records_with_start_date}")
#         print(f"Records without start_date: {records_without_start_date}")

#     except Exception as e:
#         print(f"Error processing DAT price file: {e}")
#         return False  # Return False if an error occurs
    
# PROCESS_FULL = START_DATE_CHECKER(FULL_DECOMPRESSED_FILES, 'FULL')


# test = GET_PRODUCT_TMPL_ID()
# print(test)

# matched = PROCESS_DAT_PRICE_FILE(compressed_dir, products_codes)
# if matched:
#     #UPDATE_VENDOR_PRICELIST(output_json_path)
#     print("Files processed successfully. Proceeding to update prices.")
# else:
#     print("Error processing the DAT files. Prices will not be updated.")
