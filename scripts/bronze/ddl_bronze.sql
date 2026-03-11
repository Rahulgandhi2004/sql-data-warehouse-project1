/*
==================================================
DDL Scripts:Create Bronze tables
==================================================

Script Purpose:
	This is scripts is used to create atables 
	and schema,dropping existing tables if they
	are Exist.
	Run this scripts to re_define the DDL structue
	of 'bronze' tables.

==================================================

*/

USE master;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;


CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;


IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
DROP TABLE bronze.crm_cust_info;
create table bronze.crm_cust_info(
 cst_id int,
 cst_key NVARCHAR(50),
 cst_firstname NVARCHAR(50),
 cst_lastname NVARCHAR(50),
 cst_marital_status NVARCHAR(1),
 cst_gndr NVARCHAR(1),
 cst_create_date date
);


IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
DROP TABLE bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost NVARCHAR(50), 
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
);

IF OBJECT_ID(' bronze.crm_sales_details','U') IS NOT NULL
DROP TABLE  bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT, 
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
DROP TABLE bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50)

);

IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
DROP TABLE bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)

);

IF OBJECT_ID('bronze.erp_px_cat_g1v2','U') IS NOT NULL
DROP TABLE bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
ID NVARCHAR(50),
cat NVARCHAR(50),
subcat  NVARCHAR(50),
maintanance  NVARCHAR(50)
);

