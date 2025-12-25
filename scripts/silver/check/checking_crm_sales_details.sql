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
