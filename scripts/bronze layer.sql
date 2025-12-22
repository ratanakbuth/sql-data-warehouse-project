Create or Alter Procedure bronze.load_bronze As
BEGIN
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		Print '=========================================';
		Print 'Loading Bronze Layer';
		Print '=========================================';

		Print '-----------------------------------------';
		Print 'Loading CRM Tables';
		Print '-----------------------------------------';

		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_cust_info';
		Truncate Table bronze.crm_cust_info
	
		Print '>> Inserting Data Into: bronze.crm_cust_info';
		Bulk Insert bronze.crm_cust_info
		From 'C:\Users\GL\Desktop\Data Science\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		With (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '>> ---------------'

		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_prd_info';
		Truncate Table bronze.crm_prd_info
	
		Print '>> Inserting Data Into: bronze.crm_prd_info';
		Bulk Insert bronze.crm_prd_info
		From 'C:\Users\GL\Desktop\Data Science\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		With (
			FirstRow = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '>> ---------------'

		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.crm_sales_details';
		Truncate Table bronze.crm_sales_details

		Print '>> Inserting Data Into: bronze.crm_sales_details';
		Bulk Insert bronze.crm_sales_details
		From 'C:\Users\GL\Desktop\Data Science\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		With (
			FirstRow = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '>> ---------------'

		Print '-----------------------------------------';
		Print 'Loading ERP Tables';
		Print '-----------------------------------------';

		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_cust_az12';
		Truncate Table bronze.erp_cust_az12
	
		Print '>> Inserting Data Into: bronze.erp_cust_az12';
		Bulk Insert bronze.erp_cust_az12
		From 'C:\Users\GL\Desktop\Data Science\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		With (
			FirstRow = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '>> ---------------'

		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_loc_a101';
		Truncate Table bronze.erp_loc_a101
	
		Print '>> Inserting Data Into: bronze.erp_loc_a101';
		Bulk Insert bronze.erp_loc_a101
		From 'C:\Users\GL\Desktop\Data Science\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		With (
			FirstRow = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '>> ---------------'

		SET @start_time = GETDATE();
		Print '>> Truncating Table: bronze.erp_px_cat_g1v2';
		Truncate Table bronze.erp_px_cat_g1v2
	
		Print '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		Bulk Insert bronze.erp_px_cat_g1v2
		From 'C:\Users\GL\Desktop\Data Science\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		With (
			FirstRow = 2,
			FIELDTERMINATOR = ',',
			Tablock
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		Print '>> ---------------'

		SET @batch_end_time = GETDATE();
		PRINT '==============================================='
		PRINT 'Loading Bronze Layer is Completed'
		PRINT '   - Total Load Duration: ' + Cast(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==============================================='
	END TRY
	BEGIN CATCH
		PRINT '==============================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + Error_Message();
		PRINT 'Error Message' + Cast (Error_Number() AS NVARCHAR);
		PRINT 'Error Message' + Cast (Error_State() AS NVARCHAR);
		PRINT '==============================================='
	END CATCH
END
