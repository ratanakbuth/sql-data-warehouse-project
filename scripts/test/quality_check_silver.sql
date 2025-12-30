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
-- Checking and Testing silver.crm_prd_info
-- ======================================================================================================
