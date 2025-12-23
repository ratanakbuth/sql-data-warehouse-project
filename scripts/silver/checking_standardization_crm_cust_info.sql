-- Check for unwanted spaces
-- Expection: No results
select
	cst_firstname
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select
	cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

select
	cst_gndr
from bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr)

select
	cst_key
from bronze.crm_cust_info
where cst_key != TRIM(cst_key)

-- Check for Nulls or Duplicates in the primary key
-- Expectatino: No result

select 
	cst_id,
	count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

-- Data Standardization and Consistency
Select DISTINCT cst_gndr
From bronze.crm_cust_info

Select DISTINCT cst_marital_status
From bronze.crm_cust_info

-- Check for unwanted spaces
-- Expection: No results
select
	cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

select
	cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

select
	cst_gndr
from silver.crm_cust_info
where cst_gndr != TRIM(cst_gndr)

select
	cst_key
from silver.crm_cust_info
where cst_key != TRIM(cst_key)

-- Check for Nulls or Duplicates in the primary key
-- Expectatino: No result

select 
	cst_id,
	count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

-- Data Standardization and Consistency
Select DISTINCT cst_gndr
From silver.crm_cust_info

Select DISTINCT cst_marital_status
From silver.crm_cust_info

Select * from silver.crm_cust_info
