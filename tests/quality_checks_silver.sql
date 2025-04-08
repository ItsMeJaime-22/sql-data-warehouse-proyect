/*
=================================================================================================================================
Quality Checks
=================================================================================================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy,
	and standardization across the 'silver' schema. It includes checks for:
	-	Null or duplicate primary keys.
	-	Unwanted spaces in string fields.
	-	Data standardization and consistency.
	-	Invalid date ranges and orders.
	-	Data consistency between related fields.

Usage Notes:
	-	Run these checks after data loading silver layer.
	-	Investigate and resolve any discrepancies found during the checks.
=================================================================================================================================
*/


======================================================================================================================================================
'Checking: silver.crm_cust_info'
======================================================================================================================================================
  
-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id, COUNT (*) AS CountRows
FROM bronze.crm_cust_info
GROUP BY cst_id
--ORDER BY 2 DESC;
HAVING COUNT(*) > 1;
-- Tenemos 6 valores en la columna cst_id que existen en m�s de 1 fila, es decir no son valores unicos.

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id IN ('29449', '29473', '29433', '29483', '29466') OR cst_id IS NULL
ORDER BY 1 DESC;


SELECT cst_id, COUNT (*) AS CountRows
FROM silver.crm_cust_info
GROUP BY cst_id
--ORDER BY 2 DESC;
HAVING COUNT(*) > 1;
-- Tenemos 6 valores en la columna cst_id que existen en m�s de 1 fila, es decir no son valores unicos.
-- Validaci�n: Tabla Silver, sin errores.

SELECT *
FROM silver.crm_cust_info
WHERE cst_id IN ('29449', '29473', '29433', '29483', '29466') OR cst_id IS NULL
ORDER BY 1 DESC;
-- Validaci�n: Tabla Silver, sin errores.

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;
-- Aqui podemos ver valores como n/a, Male y Female, que son los que hemos transformado.

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;
-- Aqui podemos ver valores como Single y Married, que son los que hemos transformado.
-- Pero no vemos valores n/a, debido a que estos han sido eliminados anteriormente por tener fechas antiguas.

SELECT *
FROM bronze.crm_cust_info
WHERE cst_marital_status IS NULL;
-- Aqui podemos comprobar que estos valores eran duplicados y tenian fechas antiguas, por ello fueron eliminadas con un script.


-- Nos permite saber si tenemso valores duplicados en cierta columna.
SELECT prd_id, COUNT(*) AS CountRows
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1;

======================================================================================================================================================
'Checking: silver.crm_prd_info'
======================================================================================================================================================

-- Validamos si los los valores de la columna "prd_key" ("bronze.crm_prd_info"), se encuentran en la columna "id" ("bronze.erp_px_cat_g1v2").
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
(SELECT DISTINCT id
FROM bronze.erp_px_cat_g1v2);

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE id = 'CO_PE';
-- Podemos observar que este valor no se encuentra en nuestra tabla "bronze.erp_px_cat_g1v2".


SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (
SELECT sls_prd_key
FROM bronze.crm_sales_details); -- Podemos validar que datos de la columna "SUBSTRING(prd_key, 7, LEN(prd_key))", no se encuentran en la columna "sls_prd_key"
--WHERE sls_prd_key LIKE 'FR-R9%'); -- Validamos cada dato, para ver si realmente no existe en la columna "sls_prd_key" referente a la tabla "bronze.crm_sales_details".

SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) IN (
SELECT sls_prd_key
FROM bronze.crm_sales_details); -- Podemos validar que datos de la columna "SUBSTRING(prd_key, 7, LEN(prd_key))", si se encuentran en la columna "sls_prd_key"

SELECT
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm); -- Validamos si tenemos espacios en blanco referente a la columna "prd_nm".


-- Check for NULLs or Negative Numbers
-- Expectation: No Results

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL; -- Nos permite saber si en la columna de costos "prd_cost", tenemos valores negativos o nulos.

SELECT DISTINCT(prd_line) AS prd_line
FROM bronze.crm_prd_info; -- Nos permite saber los valores unicos de la columna "prd_line".

======================================================================================================================================================
'Checking: silver.crm_sales_details'
======================================================================================================================================================

-- Check for Invalid Dates

SELECT
NULLIF(sls_order_dt, 0) AS sls_order_dt, LEN(sls_order_dt) AS Countsls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101 OR sls_order_dt < 19000101;

SELECT
NULLIF(sls_ship_dt, 0) AS sls_ship_dt, LEN(sls_ship_dt) AS Countsls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8
OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101;

SELECT 
NULLIF(sls_due_dt, 0) AS sls_due_dt, LEN(sls_due_dt) AS Countsls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101 OR sls_due_dt < 19000101;

SELECT *
FROM bronze.crm_sales_details;

-- Check for Invalid Date Orders

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt; -- Validamos si la fecha de creación es mayor que la fecha de envio o mayor a la fecha de entrega.

======================================================================================================================================================
'Checking: silver.erp_cust_az12'
======================================================================================================================================================

-- Validamos si todos los valores de la columna "cid" - tabla "bronze.erp_cust_az12" coinciden con la columna "cst_key" - tabla "silver.crm_cust_info".

SELECT
CASE WHEN LEN(cid) = 13 THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN LEN(cid) = 13 THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END NOT IN (SELECT cst_key FROM silver.crm_cust_info);

-- Validamos que no haya caracteres diferente a 10 en la columna "bdate".
SELECT *
FROM bronze.erp_cust_az12
WHERE LEN(bdate) != 10;

-- Validamos que las fechas no tengan más de 100 años de antiguedad y la fecha no sea mayor a la fecha actual.

SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Validamos la calidad de datos de nuestra columna "gen"

SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

======================================================================================================================================================
'Checking: silver.erp_loc_a101'
======================================================================================================================================================

-- Validación que valores de la columna "cid" - tabla "bronze.erp_loc_a101", no se encuentran en la columna "cst_key" - tabla "silver.crm_cust_info" :

SELECT
REPLACE(cid, '-', '') AS cid,
cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN 
(SELECT cst_key FROM silver.crm_cust_info);

-- Validamos la columna "cntry":

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY 1;

-- Validamos los cambios en la columna "cntry":

SELECT DISTINCT 
cntry AS old_cntry,
CASE WHEN TRIM(UPPER(cntry)) IN ('USA','UNITED STATES', 'US') THEN 'United States'
	WHEN TRIM(UPPER(cntry)) IN ('DE') THEN 'Germany'
	WHEN TRIM(UPPER(cntry)) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY 1;  

======================================================================================================================================================
'Checking: silver.erp_px_cat_g1v2'
======================================================================================================================================================

SELECT DISTINCT LEN(id) AS NumCaract
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT LEN(cat_id) AS NumCaract
FROM silver.crm_prd_info;

-- Validamos si tenemos espacios al inicio o final en las columnas "cat", "subcat", "maintenance":

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Validamos que las columnas no tengan datos atipicos, que luego debemos estandarizar.

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2
ORDER BY 1;

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;
