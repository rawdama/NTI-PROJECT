-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

if OBJECT_ID('gold.dim_products','v') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS produt_number,
pn.prd_nm AS product_name,
pn.cat_key AS catagory_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance AS maintenance,
pn.prd_cost AS product_cost,
pn.prd_line  AS product_line,
pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_key=pc.id
WHERE prd_end_dt is NULL --filter out historical data
GO




-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
if OBJECT_ID('gold.dim_customers','v') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_material_status AS material_status,
CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr --CRM is the master for gender
ELSE COALESCE(ca.gen,'n/a')
END AS  gender,
ca.bdate AS birthdate,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid
GO

-- =============================================================================
--Create Fact Table: gold.fact_sales
-- =============================================================================
if OBJECT_ID('gold.fact_sales','v') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_nymber,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS sales_quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
on sd.sls_prd_key=pr.produt_number
LEFT JOIN gold.dim_customers cu
ON SD.sls_cust_id=cu.customer_id
GO





