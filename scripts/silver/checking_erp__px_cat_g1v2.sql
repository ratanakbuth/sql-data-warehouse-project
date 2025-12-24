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
