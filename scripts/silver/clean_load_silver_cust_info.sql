Truncate Table silver.crm_cust_info
Insert Into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
select
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname, -- Removing unwanted spaces
TRIM(cst_lastname) as cst_lastname,
CASE when Upper(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	when Upper(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	Else 'n/a' -- Handling missing data
END cst_marital_status, -- Data Normalize the marital status values to readable format
CASE when Upper(TRIM(cst_gndr)) = 'F' Then 'Female'
	when Upper(TRIM(cst_gndr)) = 'M' Then 'Male'
	ELSE 'n/a' -- Handling missing data
END cst_gndr, -- Normalize the gender values to readable format
cst_create_date
From (
	select
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
	from bronze.crm_cust_info
	Where cst_id is NOT NULL  -- Remove Duplicate
)t Where flag_last = 1 -- Select the most recent record per customer
