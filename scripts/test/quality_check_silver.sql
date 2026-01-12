-- ======================================================================================================
-- Checking and Testing silver.crm_cust_info
-- ======================================================================================================
-- Checking before transforming and insert to silver table
select *
from bronze.crm_prd_info

-- Check for Nulls or Duplicates in the primary key
-- Expectatino: No result

select
	prd_id,
	count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 and prd_id is null

Select 
	prd_id,
	prd_cost
from bronze.crm_prd_info
where prd_cost is null

Select 
	prd_end_dt
from bronze.crm_prd_info
where prd_end_dt is null

-- Check for unwanted spaces
-- Expection: No results

Select 
	prd_key
from bronze.crm_prd_info
where prd_key != TRIM(prd_key)

Select 
	prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- Data Standardization and Consistency

Select Distinct prd_line
from bronze.crm_prd_info

-- Checking silver table prd_info after inserting
select *
from silver.crm_prd_info

-- Check for Nulls or Duplicates in the primary key
-- Expectatino: No result

select
	prd_id,
	count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 and prd_id is null

Select 
	prd_id,
	prd_cost
from bronze.crm_prd_info
where prd_cost is null or prd_cost < 0

Select 
	prd_end_dt
from bronze.crm_prd_info
where prd_end_dt is null

-- Check for unwanted spaces
-- Expection: No results

Select 
	prd_key
from bronze.crm_prd_info
where prd_key != TRIM(prd_key)

Select 
	prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- Data Standardization and Consistency

Select Distinct prd_line
from silver.crm_prd_info

-- Check for Invalid Date Orders

Select * 
From silver.crm_prd_info
Where prd_end_dt < prd_start_dt

--Testing prd_end_dt partition
Select
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (Partition by prd_key Order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info
Where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')

-- ======================================================================================================
-- Checking and Testing silver.crm_prd_info
-- ======================================================================================================

-- Checking before transforming and insert to silver table
select *
from bronze.crm_prd_info

-- Check for Nulls or Duplicates in the primary key
-- Expectatino: No result

select
	prd_id,
	count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 and prd_id is null

Select 
	prd_id,
	prd_cost
from bronze.crm_prd_info
where prd_cost is null

Select 
	prd_end_dt
from bronze.crm_prd_info
where prd_end_dt is null

-- Check for unwanted spaces
-- Expection: No results

Select 
	prd_key
from bronze.crm_prd_info
where prd_key != TRIM(prd_key)

Select 
	prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- Data Standardization and Consistency

Select Distinct prd_line
from bronze.crm_prd_info

-- Checking silver table prd_info after inserting
select *
from silver.crm_prd_info

-- Check for Nulls or Duplicates in the primary key
-- Expectatino: No result

select
	prd_id,
	count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 and prd_id is null

Select 
	prd_id,
	prd_cost
from bronze.crm_prd_info
where prd_cost is null or prd_cost < 0

Select 
	prd_end_dt
from bronze.crm_prd_info
where prd_end_dt is null

-- Check for unwanted spaces
-- Expection: No results

Select 
	prd_key
from bronze.crm_prd_info
where prd_key != TRIM(prd_key)

Select 
	prd_nm
from bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- Data Standardization and Consistency

Select Distinct prd_line
from silver.crm_prd_info

-- Check for Invalid Date Orders

Select * 
From silver.crm_prd_info
Where prd_end_dt < prd_start_dt

--Testing prd_end_dt partition
Select
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (Partition by prd_key Order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info
Where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')

-- ======================================================================================================
-- Checking and Testing silver.crm_sales_details
-- ======================================================================================================
Select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
From silver.crm_sales_details

Select *
From silver.crm_cust_info

Select *
From silver.crm_prd_info

-- Checking for null & duplicate in primary key
-- Expectation: No result
Select
	sls_prd_key,
	COUNT(*)
From silver.crm_sales_details
group by sls_prd_key
having count(*) > 1 and sls_prd_key is null

-- Checking for unwanted space
-- Expectation: No result
Select 
	sls_prd_key
From silver.crm_sales_details
Where sls_prd_key != TRIM(sls_prd_key)

-- Check for Invalid Dates
Select 
	NULLIF(sls_due_dt, 0) as sls_due_dt
From bronze.crm_sales_details
Where sls_due_dt <= 0 
or LEN(sls_due_dt) != 8 
or sls_due_dt > 20500101 
or sls_due_dt < 19000101

Select *
From silver.crm_sales_details
Where sls_order_dt > sls_ship_dt or sls_ship_dt > sls_due_dt

-- Checking Data Consistency: Between Sales, Quantity, & Price
-- >> Sales = Quantity * Price
-- Expectation: Values must not be null, zero or negative

Select 
	sls_sales,
	sls_quantity,
	sls_price
From silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
Order by sls_sales,	sls_quantity, sls_price

/* Many negative from above result:
Rules to transform above data:
	- If Sales is negative, zero or null, derive it using Qty & Price.
	- If Price is zero or null, calculated it using Sales & Qty.
	- If Price is negative, convert it to a positive value.
*/
Select Distinct
	sls_sales as old_sls_sales,
	sls_quantity,
	sls_price as old_sls_price,
	CASE WHEN sls_sales is NULL or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
		Then sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	CASE WHEN sls_price is NULL or sls_price <=0
		Then sls_sales / NULLIF(sls_quantity , 0)
		ELSE sls_price
	END AS sls_price
From bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
Order by sls_sales,	sls_quantity, sls_price

select * from silver.crm_sales_details

-- ======================================================================================================
-- Checking and Testing silver.erp_cust_az12
-- ======================================================================================================
select
	cid,
	CASE WHEN cid like 'NAS%' Then SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END cid,
	bdate,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	gen,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		Else 'n/a'
	END as gen
from bronze.erp_cust_az12

-- Identify Out-of-Range Dates

Select Distinct
bdate
From silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE()

Select distinct
gen
from silver.erp_cust_az12

Select * from silver.erp_cust_az12
	
-- ======================================================================================================
-- Checking and Testing silver.erp_loc_a101
-- ======================================================================================================
Select
	REPLACE(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' Then 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') Then 'United States'
		WHEN TRIM(cntry) = '' or cntry is NULL then 'n/a'
		Else TRIM(cntry)
	END as cntry
from bronze.erp_loc_a101

--where REPLACE(cid, '-', '') NOT IN
--(Select cst_key from silver.crm_cust_info)

select cst_key from silver.crm_cust_info

-- Data Standardation and Consistency
Select Distinct
cntry
From silver.erp_loc_a101
Order by cntry

Select * from silver.erp_loc_a101
	
-- ======================================================================================================
-- Checking and Testing silver.erp_px_cat_g1v2
-- ======================================================================================================
Select 
	id,
	cat,
	subcat,
	maintenance
from bronze.erp_px_cat_g1v2

-- Checking for unwanted spaces
Select * from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat) or maintenance != TRIM(maintenance)

-- Data Standardizatino & Consistency
Select Distinct
maintenance
from bronze.erp_px_cat_g1v2

