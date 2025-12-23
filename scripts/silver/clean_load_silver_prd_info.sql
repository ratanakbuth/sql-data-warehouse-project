Select
prd_id,
prd_key,
Replace(SUBSTRING(prd_key, 1, 5), '-','_' ) as cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
From bronze.crm_prd_info
