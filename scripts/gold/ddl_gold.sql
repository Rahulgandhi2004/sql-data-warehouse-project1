/*
============================================================================
DDL Script: Create Gold Views
============================================================================
Script Purpose:
This script creates views for the Gold layer in the data warehouse.
The Gold layer represents the final dimension and fact tables (Star Schema)

Each view performs transformations and combines data from the Silver layer
to produce a clean, enriched, and business-ready dataset.

Usage:
- These views can be queried directly for analytics and reporting.
============================================================================
*/

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER( ORDER BY cst_id) AS customer_key,
	ci.cst_id as  Customer_id,
	ci.cst_key as Customer_number,
	ci.cst_firstname as First_name,
	ci.cst_lastname as Last_name,
	CASE WHEN ci.cst_gndr!= 'n/a' THEN ci.cst_gndr
		 ELSE COALESCE(ca.gen,'n/a')
		 END as Gender,
	la.cntry as Country,
	ca.bdate as Birth_date,
	ci.cst_marital_status as marital_status,
	ci.cst_create_date as create_date


FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid

--------------------------------------------------------
CREATE VIEW gold.dim_products AS(
SELECT
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) AS Product_key,
prd_id as Product_ID,
prd_key as product_Number,
prd_nm as Product_Name,
cat_id as Catagory_ID,
pc.cat as Catagory ,
pc.subcat as Sub_Catagory,
pc.maintenance as Maintenance,
prd_cost as Product_Cost, 
prd_line as Product_line,
prd_start_dt as StartDate

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE pn.prd_end_dt IS NULL)


-------------------------------------------------------------
CREATE VIEW gold.fact_sales AS(
SELECT 
sls_ord_num as Order_Number,
pr.Product_key as Product_Key,
cu.customer_key as Customer_key,
sls_order_dt as Order_Date,
sls_ship_dt as Shiping_Date,
sls_due_dt as Due_datae,
sls_sales as Sales,
sls_quantity as Quantity,
sls_price as Price
FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_products pr
	ON sd.sls_prd_key=pr.product_Number
	LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id=cu.Customer_id)
