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
INSERT INTO silver.crm_prd_info (
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
