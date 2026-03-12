============================================================================
Quality Checks
============================================================================
Script Purpose:
This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schema. It includes checks for:
- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardization and consistency.
- Invalid date ranges and orders.
- Data consistency between Tflated fields.

Usage Notes:
- Run these checks after data loading Silver Layer.
- Investigate and resolve any discrepancies found during the checks.
============================================================================
--+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+--
---------------------------------------------------
--Check for Duplicate in Primary key
--Expectation : No Result

SELECT cst_id,COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1;

--Check for Unwanted Space
--Expectation: No results
--Solution: TRIM(prd_line)
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--Check for NULLs or Negative Number
--Expectation: No Result 
--Solution: ISNULL(prd_cost,0) 

SELECT prd_cost
FROM silver.crm_prd_info 
WHERE prd_cost<0 OR prd_cost IS NULL;


--Data Standardization & Consistency

SELECT DISTINCT prd_line
from silver.crm_prd_info;


--Check For invalid date Order
SELECT * 
FROM silver.crm_prd_info
WHERE prd_end_dt<prd_start_dt;

--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--

--Check Invalid dates--
SELECT 
NULLIF(sls_ship_dt,0) sla_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt<=0
OR sls_ship_dt>20500101
OR sls_ship_dt<19000101

--Check For invalid date Order
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt>sls_ship_dt OR
sls_order_dt>sls_due_dt

SELECT 
sls_sales,
sls_quantity,
sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price OR
sls_sales IS NULL OR sls_quantity IS NULL  OR sls_price IS NULL OR
sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0


