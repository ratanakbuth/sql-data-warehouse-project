Truncate Table silver.erp_loc_a101
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
