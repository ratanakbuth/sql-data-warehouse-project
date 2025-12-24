Truncate Table silver.crm_sales_details
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
-- Checking whether sls_cust_id can related with cst_id and sls_prd_key with prd_key
-- Where sls_cust_id NOT IN (Select cst_id From silver.crm_cust_info)
-- Where sls_prd_key NOT IN (Select prd_key From silver.crm_cust_info)
