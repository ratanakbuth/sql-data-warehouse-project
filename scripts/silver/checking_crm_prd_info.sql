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

