# abomar-api-jd-odoo
Repository for the Abomar API Integration System Development Project for bridging John Deere inventory systems and Odoo.

# RUN DEV ENVIRONMENT
docker build -t odoo-app .
docker run -v //c/laragon/www/abomar-api-jd-odoo/Files:/app/main/Files odoo-app
