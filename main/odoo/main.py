from functions import get_model_fields, PROCESS_DAT_PRICE_FILE, UPDATE_VENDOR_PRICELIST, GET_PRODUCT_TMPL_ID, GET_CURRENCY

compressed_dir = "../../Files/Decompressed_Files"
output_json_path = "matched_products.json"
PROCESS = PROCESS_DAT_PRICE_FILE(compressed_dir)

# matched = PROCESS_DAT_PRICE_FILE(compressed_dir, products_codes)
# if matched:
#     #UPDATE_VENDOR_PRICELIST(output_json_path)
#     print("Files processed successfully. Proceeding to update prices.")
# else:
#     print("Error processing the DAT files. Prices will not be updated.")
