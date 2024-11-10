FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

ENV NAME World

# Make the entry point script executable
RUN chmod +x /app/entrypoint.sh

# Use the entry point script to run both scripts
ENTRYPOINT ["/app/entrypoint.sh"]

# FROM python:3.9-slim

# WORKDIR /app

# COPY . /app

# RUN pip install --no-cache-dir -r requirements.txt

# EXPOSE 80

# ENV NAME World

# # Use CMD to run both scripts sequentially
# CMD ["sh", "-c", "python /app/main/odoo/odoo_stock_data.py && python /app/main/main.py"]
