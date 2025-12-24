/*
=========================================================================================================
Stored Procedure: Load Silver Layer (Source -> Silver)
=========================================================================================================
Script Purpose:
	This stored procedure loads data into the 'silver' schema from external CSV files.
	It performs the following actions:

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	Exec silver.load_silver;
=========================================================================================================
*/


Create OR Alter Procedure silver.load_silver As
Begin
	Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY
			SET @batch_start_time = GETDATE();
			Print '=========================================';
			Print 'Loading Silver Layer';
			Print '=========================================';

			Print '-----------------------------------------';
			Print 'Transforming & Loading CRM Tables';
			Print '-----------------------------------------';

			SET @start_time = GETDATE();
			Print '>> Truncating Table: silver.crm_cust_info';
			Truncate Table silver.crm_cust_info

			Print '>> Inserting Data Into: silver.crm_cust_info';
			Insert Into silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date)
			select
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname, -- Removing unwanted spaces
			TRIM(cst_lastname) as cst_lastname,
			CASE when Upper(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				when Upper(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				Else 'n/a' -- Handling missing data
			END cst_marital_status, -- Data Normalize the marital status values to readable format
			CASE when Upper(TRIM(cst_gndr)) = 'F' Then 'Female'
				when Upper(TRIM(cst_gndr)) = 'M' Then 'Male'
				ELSE 'n/a' -- Handling missing data
			END cst_gndr, -- Normalize the gender values to readable format
			cst_create_date
			From (
				select
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
				from bronze.crm_cust_info
				Where cst_id is NOT NULL  -- Remove Duplicate
			)t Where flag_last = 1 -- Select the most recent record per customer

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			Print '>> ---------------'

			SET @start_time = GETDATE();
			Print '>> Truncating Table: silver.crm_prd_info';
			Truncate Table silver.crm_prd_info

			Print '>> Inserting Data Into: silver.crm_prd_info';
			Insert Into silver.crm_prd_info (
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			Select
				prd_id,
				Replace(SUBSTRING(prd_key, 1, 5), '-','_' ) as cat_id,	-- Extract category ID
				SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,			-- Extract product key
				prd_nm,
				ISNULL(prd_cost, 0) as pro_cost,	-- Transform Null to 0
				CASE UPPER(TRIM(prd_line))
					WHEN 'M' Then 'Mountain'
					WHEN 'S' Then 'Other Sales'
					WHEN 'R' Then 'Road'
					WHEN 'T' Then 'Touring'
					ELSE 'n/a'	-- Missing data transform to n/a
				END as prd_line,	-- Map product line codes to descriptive values
				CAST (prd_start_dt as DATE) as prd_start_dt,
				CAST(
					LEAD(prd_start_dt) OVER (Partition by prd_key Order by prd_start_dt)-1 as DATE
				) as prd_end_dt	-- Calculate end date as one day before the next start date
			From bronze.crm_prd_info

			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			Print '>> ---------------'

			SET @start_time = GETDATE();
			Print '>> Truncating Table: silver.crm_sales_details';
			Truncate Table silver.crm_sales_details

			Print '>> Inserting Data Into: silver.crm_sales_details';
			Insert Into silver.crm_sales_details (
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
			Select 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL -- Handle invalid data
					 ELSE CAST(CAST(sls_order_dt AS VARCHAR) as DATE) -- transform data type
				END as sls_order_dt,
				CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
					 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) as DATE)
				END as sls_ship_dt,
				CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
					 ELSE CAST(CAST(sls_due_dt AS VARCHAR) as DATE)
				END as sls_due_dt,
				CASE WHEN sls_sales is NULL or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
					Then sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
				sls_quantity,
				CASE WHEN sls_price is NULL or sls_price <=0
					Then sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price
				END AS sls_price -- Derive price if original value is invalid
			From bronze.crm_sales_details
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			Print '>> ---------------'

			Print '-----------------------------------------';
			Print 'Transforming & Loading ERP Tables';
			Print '-----------------------------------------';

			SET @start_time = GETDATE();
			Print '>> Truncating Table: silver.erp_cust_az12';
			Truncate Table silver.erp_cust_az12

			Print '>> Inserting Data Into: silver.erp_cust_az12';
			Insert Into silver.erp_cust_az12(
				cid,
				bdate,
				gen
			)
			select
				CASE WHEN cid like 'NAS%' Then SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
					ELSE cid
				END cid,
				CASE WHEN bdate > GETDATE() THEN NULL
					ELSE bdate
				END AS bdate, -- Set future birthdates to NULL
				CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
					WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
					Else 'n/a'
				END as gen -- Normalize gender values and handle unknow cases
			from bronze.erp_cust_az12
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			Print '>> ---------------'
			
			SET @start_time = GETDATE();
			Print '>> Truncating Table: silver.erp_loc_a101';
			Truncate Table silver.erp_loc_a101

			Print '>> Inserting Data Into: silver.erp_loc_a101';
			Insert Into silver.erp_loc_a101(
				cid,
				cntry
			)
			Select
				REPLACE(cid, '-', '') cid,  -- Handle invalid value by remove '-'
				CASE WHEN TRIM(cntry) = 'DE' Then 'Germany'
					WHEN TRIM(cntry) IN ('US', 'USA') Then 'United States'
					WHEN TRIM(cntry) = '' or cntry is NULL then 'n/a'
					Else TRIM(cntry)
				END as cntry -- Normalize and handle missing or blank country codes
			from bronze.erp_loc_a101
			SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + Cast(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			Print '>> ---------------'

			SET @start_time = GETDATE();
			Print '>> Truncating Table: silver.erp_px_cat_g1v2';
			Truncate Table silver.erp_px_cat_g1v2

			Print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
			Insert Into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
			Select 
				id,
				cat,
				subcat,
				maintenance
			from bronze.erp_px_cat_g1v2
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
