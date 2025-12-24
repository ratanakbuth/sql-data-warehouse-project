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
