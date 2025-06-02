-- Insert data from csv file to crm_customers table
COPY bronze.crm_customers
FROM 'https://raw.githubusercontent.com/HeinHtetNyi/data-warehouse-from-scratch/main/data-sources/crm/customers.csv'
(AUTO_DETECT TRUE);

-- Check data
SELECT * FROM bronze.crm_customers LIMIT 10;
--------------------------------------------------

COPY bronze.crm_products
FROM 'https://raw.githubusercontent.com/HeinHtetNyi/data-warehouse-from-scratch/main/data-sources/crm/products.csv'
(AUTO_DETECT TRUE);

-- Check data
SELECT * FROM bronze.crm_products LIMIT 10;
--------------------------------------------------

COPY bronze.crm_sale_details
FROM 'https://raw.githubusercontent.com/HeinHtetNyi/data-warehouse-from-scratch/main/data-sources/crm/sale-details.csv'
(AUTO_DETECT TRUE);

-- Check data
SELECT * FROM bronze.crm_sale_details LIMIT 10;
--------------------------------------------------

COPY bronze.erp_customers
FROM 'https://raw.githubusercontent.com/HeinHtetNyi/data-warehouse-from-scratch/main/data-sources/erp/customers.csv'
(AUTO_DETECT TRUE);

-- Check data
SELECT * FROM bronze.erp_customers LIMIT 10;
--------------------------------------------------

COPY bronze.erp_locations
FROM 'https://raw.githubusercontent.com/HeinHtetNyi/data-warehouse-from-scratch/main/data-sources/erp/locations.csv'
(AUTO_DETECT TRUE);

-- Check data
SELECT * FROM bronze.erp_locations LIMIT 10;
--------------------------------------------------

COPY bronze.erp_product_categories
FROM 'https://raw.githubusercontent.com/HeinHtetNyi/data-warehouse-from-scratch/main/data-sources/erp/product-categories.csv'
(AUTO_DETECT TRUE);

-- Check data
SELECT * FROM bronze.erp_product_categories LIMIT 10;
