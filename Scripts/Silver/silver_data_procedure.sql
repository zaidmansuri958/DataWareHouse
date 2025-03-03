CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN

	DECLARE @start_time DATETIME ,@end_time DATETIME ,@batch_start_time DATETIME ,@batch_end_time DATETIME;
	BEGIN TRY 
		SET @batch_start_time=GETDATE()
		PRINT '===============================================';
		PRINT 'LOADING SILVER TABLES';
		PRINT '===============================================';

		PRINT '----------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '-----------------------------------------------------';


		SET @start_time=GETDATE()

			PRINT '>> TRUNCATING TABLE : SILVER.CRM_CUST_INFO';
			TRUNCATE TABLE silver.crm_cust_info;
			PRINT '>> INSERTING DATA INTO SILVER.CRM_CUST_INFO';
			INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
			)
			SELECT 
			cst_id,
			cst_key,
			TRIM (cst_firstname) AS cst_firstname,
			TRIM (cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr,

			cst_create_date FROM (select * , ROW_NUMBER() OVER (partition by cust_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info where cst_id is not null)t
			WHERE flag_last=1;
			SET @end_time=GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';


			SET @start_time=GETDATE();
				PRINT '>> TRUNCATING TABLE : SILVER.CRM_PRD_INFO';
				TRUNCATE TABLE silver.crm_prd_info;
				PRINT '>> INSERTING DATA INTO : SILVER.CRM_PRD_INFO';
				INSERT INTO silver.crm_prd_info(
					prd_id,
					cat_id,
					prd_key,
					prd_nm,
					prd_cost,
					prd_line,
					prd_start_dt,
					prd_end_dt
				
				)
				SELECT 
				prd_id,
				REPLACE(SUBSTRING(prd_key,1,5),'-','_')AS cat_id,
				SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
				prd_nm,
				ISNULL(prd_cost,0) AS prd_cost,
				CASE 
					WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
					WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
					WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
					WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
					ELSE 'n/a'
				END AS prd_line,
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_Start_dt)-1 AS DATE)AS prd_end_dt
				FROM bronze.crm_prd_info;
				SET @end_time=GETDATE();

				PRINT '>> LOAD DURATION : '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
				PRINT '---------------------------';


				SET @start_time=GETDATE();
				PRINT '>> TRUNCATING TABLE SILVER.CRM_SALES_DETAILS';
				TRUNCATE TABLE silver.crm_sales_details;
				PRINT 'INSERT DATA INTO : SILVER.CRM_SALES_DETAILS';
				INSERT INTO silver.crm_sales_details(
					sls_ord_num,
					sls_prd_key,
					sls_cust_id,
					sls_order_dt,
					sls_ship_dt,
					sls_due_dt,
					sls_sales,
					sls_quantity,
					sls_price
				)SELECT  
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE 
					WHEN sls_order_dt= 0 or len(sls_order_dt) !=8 THEN NULL 
					ELSE CAST(CAST (sls_order_dt as VARCHAR) as date)
				END AS sls_order_dt,
				CASE 
					WHEN sls_ship_dt=0 OR LEN(sls_ship_dt) != 8 THEN NULL 
					ELSE CAST(CAST(sls_ship_dt as VARCHAR)AS DATE) 
				END AS sls_ship_dt,
				CASE 
					WHEN sls_due_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL 
					ELSE CAST(CAST(sls_due_dt AS VARCHAR )AS DATE )
				END AS sls_due_dt,
				CASE 
					WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quntity * ABS(sls_price)
						THEN sls_quantity* ABS(sls_price)
					ELSE sls_sales 
				END AS sls_sales;
				sls_qunatity,
				CASE 
					WHEN sls_price is null or sls_price <=0
						THEN sls_sales/ NULLIF(sls_quntity,0)
					else sls_price
				end as sls_price
				FROM bronze.crm_sales_details;

				SET @end_time=GETDATE();

				PRINT '>> LOAD DURATION : '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
				PRINT '>> ---------------------------------';


				SET @start_time=GETDATE();
				PRINT '>> TRUNCATING TABLE : SILVER.ERP_CUST_AZ12';
				TRUNCATE TABLE silver.erp_cust_az12;
				PRINT '>> INSERTING DATA INTO : SILVER.ERP_CUST_AZ12';
				INSERT INTO silver.erp_cust_az12(
					cid,
					bdate,
					gen
				)
				SELECT 
					CASE 
						WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
						ELSE cid 
					END AS cid,
					CASE 
						WHEN bdate>GETDATE() then null
						else bdate 
					end as bdate,
					case 
						when upper(trim(gen)) in ('F','FEMALE') then 'Female'
						when upper(trim(gen)) in ('M','MALE') then 'Male'
						else 'n/a'
					end as gen
				from bronze.erp_cust_az12;
			set @end_time=GETDATE();

			PRINT '>> LOAD DURATION : '+CAST(DATEDIFF(SECOND,@start_time,@end_time)AS NVARCHAR)+ ' seconds';
			PRINT '>> ---------------------------------';

			SET @start_time=GETDATE();
			PRINT '>> TRUNCATING TABLE : SIVLER.ERP_LOC_A101';
			TRUNCATE TABLE silver.erp_loc_a1o1;
			PRINT '>> INSERTING DATA INTO : SILVER.ERP_LOC_A101';
			INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
			)
			SELECT 
			REPLACE(cid,'-','_')AS cid,
			CASE 
				when TRIM (cntry) = 'DE' then 'Germany'
				when TRIM (cntry) IN ('US','USA') then 'United States'
				when TRIM (cntry) = '' or cntry is null then 'n/a'
				else TRIM (cntry)
			end as cntry 
		from bronze.erp_loc_a101;
		set @end_time=GETDATE();
		PRINT '>> LOAD DURATION : '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>> -----------------';
		
			SET @start_time=GETDATE();
			PRINT '>> TRUNCATING TABLE : SIVLER.ERP_PX_CAT_G1V2';
			TRUNCATE TABLE silver.erp_px_cat_g2v2;
			PRINT '>> ISNERTING DATA INTO : SILVER.ERP_PX_CAT_G1V2';
			INSERT INTO silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenanace
			)
			SELECT
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2;
			SET @end_time=GETDATE();
			PRINT '>> LOAD DURATION : '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '>> -----------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH



