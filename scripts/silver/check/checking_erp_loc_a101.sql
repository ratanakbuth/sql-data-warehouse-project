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
