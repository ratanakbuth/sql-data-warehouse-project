Truncate Table silver.erp_cust_az12
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
