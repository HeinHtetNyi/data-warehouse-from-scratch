-- We got multiple customers with the same cst_id,
-- So, we need to filter the oudated customers info
WITH crm_transformed_customers AS (
  SELECT 
    cst_id, cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
  CASE
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
     ELSE 'n/a'
  END AS cst_marital_status,
  CASE
      WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
      WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
      ELSE 'n/a'
  END AS cst_gndr,
  cst_create_date,
  FROM (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
    FROM bronze.crm_customers
  )
  WHERE flag = 1
  LIMIT 10
)

INSERT INTO silver.crm_customers (
  cst_id, 
  cst_key, 
  cst_firstname, 
  cst_lastname, 
  cst_marital_status, 
  cst_gndr,
  cst_create_date
)
SELECT * FROM crm_transformed_customers;
------------------------------------------------------------

WITH crm_transformed_products AS (
  SELECT 
  prd_id,
  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
  SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
  prd_nm, 
  COALESCE(prd_cost, 0) AS prd_cost,
  CASE 
      WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
      WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
      WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
      WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
      ELSE 'n/a'
  END AS prd_line, 
  prd_start_dt:: DATE AS prd_start_dt,
  LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)::DATE - 1 AS prd_end_dt
  FROM bronze.crm_products
)
INSERT INTO silver.crm_products (
  prd_id,
  cat_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt
)
SELECT * FROM crm_transformed_products;
------------------------------------------------------------

WITH crm_transformed_sale_details AS (
  SELECT
     sls_ord_num,
     sls_prd_key,
     sls_cust_id,
     CASE WHEN  sls_order_dt = 0 OR LEN(sls_order_dt::TEXT) != 8 THEN NULL
     ELSE STRPTIME(sls_order_dt::TEXT, '%Y%m%d')::DATE
     END AS sls_order_dt,
     CASE WHEN  sls_ship_dt = 0 OR LEN(sls_ship_dt::TEXT) != 8 THEN NULL
     ELSE STRPTIME(sls_ship_dt::TEXT, '%Y%m%d')::DATE
     END AS sls_ship_dt,
     CASE WHEN  sls_due_dt = 0 OR LEN(sls_due_dt::TEXT) != 8 THEN NULL
     ELSE STRPTIME(sls_due_dt::TEXT, '%Y%m%d')::DATE
     END AS sls_due_dt,
     CASE 
       WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
       THEN sls_quantity * ABS(sls_price)
       ELSE sls_sales
     END AS sls_sales,
     sls_quantity,
     CASE  
       WHEN sls_price IS NULL OR sls_price <= 0 
       THEN sls_sales / NULLIF(sls_quantity, 0)
       ELSE sls_price
     END AS sls_price,
  FROM bronze.crm_sale_details
)
INSERT INTO silver.crm_sale_details (
      	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT * FROM crm_transformed_sale_details;
------------------------------------------------------------

WITH erp_transformed_customers AS (
	SELECT
		CASE
		  WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		  ELSE cid
		END AS cid, 
		CASE
		  WHEN bdate > CURRENT_DATE THEN NULL
		  ELSE bdate
		END AS bdate,
		CASE
		  WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		  WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		  ELSE 'n/a'
		END AS gen 
	FROM bronze.erp_customers
)
INSERT INTO silver.erp_customers (
	cid,
	bdate,
	gen
)
SELECT * FROM erp_transformed_customers;
------------------------------------------------------------

WITH erp_transformed_locations AS (
	SELECT
		REPLACE(cid, '-', '') AS cid, 
		CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry 
	FROM bronze.erp_locations
)
INSERT INTO silver.erp_locations (
	cid,
	cntry
)
SELECT * FROM erp_transformed_locations;
------------------------------------------------------------

WITH erp_transformed_prod_categories AS (
  SELECT
      id,
      cat,
      subcat,
      maintenance
  FROM bronze.erp_product_categories
)
INSERT INTO silver.erp_product_categories (
	id,
	cat,
	subcat,
	maintenance
)
SELECT * FROM erp_transformed_prod_categories;
