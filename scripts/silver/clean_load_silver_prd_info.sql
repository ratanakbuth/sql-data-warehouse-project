Truncate Table silver.crm_prd_info
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

--Checking prd_key not in sales details
--Where SUBSTRING(prd_key, 7, LEN(prd_key)) not in
--(Select sls_prd_key from bronze.crm_sales_details)

--Checking cat_id not in sales details
-- Where Replace(SUBSTRING(prd_key, 1, 5), '-','_' ) NOT IN
-- (Select Distinct id from bronze.erp_px_cat_g1v2)
