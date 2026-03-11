/*

=====================================================
Stored procedure: load Bronze layer (source-> Bronze)
=====================================================

Script Purpose:
	The Stored procedure load date into the 'bronze' 
	schema from external CSV files.
	-Truncates the brones tables loading data.
	-uses the 'BULK INSERT' command to load data from
	csv Files to bronze tables

Parameters:
	None, The Stored procedure does not accept any 
	parameters or return any vaues.

 usage Example:
	EXEC bronze.load_bronze;
=====================================================

================================================
(Bronze Layer))BULK DATA INSERTING FROM CSV FILE 

================================================

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@batchstart_time DATETIME,@batchend_time DATETIME;
	SET @batchstart_time=GETDATE();
	BEGIN TRY 
		PRINT '**********************************************';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '**********************************************';


		PRINT '==============================================';
		PRINT 'Loading CRM file data';
		PRINT '==============================================';

		SET @start_time=GETDATE();
		PRINT '>> Truncating The Table:bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info ;--Refreshing the Table , We Run Each Time

	
		PRINT '>> Inserting the data into The Table:bronze.crm_cust_info';

		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\rahulgandhi.m\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		SET @end_time=GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' Seconds';

		--SELECT * from bronze.crm_cust_info;
		--SELECT COUNT(*) from  bronze.crm_cust_info;

		SET @start_time=GETDATE();
		PRINT '>> Truncating The Table:bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info ;--Refreshing the Table , We Run Each Time
	
		PRINT '>> Inserting the data into The Table: bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info 
		from 'C:\Users\rahulgandhi.m\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		SET @end_time=GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) + ' Seconds';

		--SELECT * from bronze.crm_prd_info;
		--SELECT COUNT(*) from  bronze.crm_prd_info;

		SET @start_time=GETDATE();
		PRINT '>> Truncating The Table:bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details ;--Refreshing the Table , We Run Each Time
		PRINT '>>Inserting the data into The Table:bronze.crm_sales_details';

		BULK INSERT bronze.crm_sales_details 
		from 'C:\Users\rahulgandhi.m\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		SET @end_time=GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' Seconds';
		--SELECT * from bronze.crm_sales_details;
		--SELECT COUNT(*) from  bronze.crm_sales_details;

		PRINT '==============================================';
		PRINT 'Loading ERP file data';
		PRINT '==============================================';

		SET @start_time=GETDATE();
		PRINT '>> Truncating The Table:bronze.erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12 ;--Refreshing the Table , We Run Each Time
	
		PRINT '>> Inserting the data into The Table:bronze.erp_cust_az12 ';

		BULK INSERT bronze.erp_cust_az12
		from 'C:\Users\rahulgandhi.m\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		SET @end_time=GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' Seconds';
	

		--SELECT * from bronze.erp_cust_az12;
		--SELECT COUNT(*) from  bronze.erp_cust_az12;

		SET @start_time=GETDATE();
		PRINT '>> Truncating The Table:bronze.erp_loc_a101  ';
		TRUNCATE TABLE bronze.erp_loc_a101 ;--Refreshing the Table , We Run Each Time
		PRINT '>> Inserting the Table into The Table:bronze.erp_loc_a101  ';

		BULK INSERT bronze.erp_loc_a101
		from 'C:\Users\rahulgandhi.m\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
		firstrow=2,
		fieldterminator=',',
		tablock
		);
		SET @end_time=GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' Seconds';
	
		--SELECT * FROM bronze.erp_loc_a101;
		--SELECT COUNT(*) FROM  bronze.erp_loc_a101;

        SET @start_time=GETDATE();
		PRINT '>> Truncating The Table:bronze.erp_px_cat_g1v2  ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting the data into The Table:bronze.erp_px_cat_g1v2  ';

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\rahulgandhi.m\Downloads\sql-data-warehouse-project (1)\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT 'Loading Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+ ' Seconds';
	
		--SELECT * FROM bronze.erp_px_cat_g1v2;
		--SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
	END TRY
	--If Any Error Occur During This part Excicution The catch block will run
	BEGIN CATCH

		PRINT '++++++++++++++++++++++++++++++++++++++++++++';
		PRINT 'ERROR OCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '++++++++++++++++++++++++++++++++++++++++++++';

	END CATCH

	SET @batchend_time=GETDATE();
    PRINT 'Bronze Layer Loading Duration: ' + CAST(DATEDIFF(second,@batchstart_time,@batchend_time)AS NVARCHAR)+ ' Seconds';


END
