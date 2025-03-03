CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
			SET @batch_start_time=GETDATE();
			PRINT '====================================================';
			PRINT 'LOADING BRONZE LAYER';
			PRINT '====================================================';
			PRINT '----------------------------------------------------';
			PRINT 'LOADING CRM TABLES';
			PRINT '-----------------------------------------------------';

			SET @start_time=GETDATE();

			PRINT '>> TRUNCATING TABLE : BRONZE.CRM_CUST_INFO';
			TRUNCATE TABLE bronze.crm_cust_info;
			PRINT '>> INSERTING DATA INTO : BRONZE.CRM_CUST_INFO';
			BULK INSERT bronze.crm_cust_info FROM 'E:\DataEngineering Projects\DataWareHouse\dataset\source_crm\cust_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

			SET @end_time=GETDATE();
			PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> -----------------';


			SET @start_time=GETDATE();

			PRINT '>> TRUNCATING TABLE : BRONZE.CRM_PRD_INFO';
			TRUNCATE TABLE bronze.crm_prd_info;
			PRINT '>> INSERTING DATA INTO : BRONZE.CRM_PRD_INFO';
			BULK INSERT bronze.crm_prd_info FROM 'E:\DataEngineering Projects\DataWareHouse\dataset\source_crm\prd_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

			SET @end_time=GETDATE();
			PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> -----------------';


			SET @start_time=GETDATE();

			PRINT '>> TRUNCATING TABLE : BRONZE.CRM_SALES_DETAILS';
			TRUNCATE TABLE bronze.crm_sales_details;
			PRINT '>> INSERTING DATA INTO : BRONZE.CRM_SALES_DETAILS';
			BULK INSERT bronze.crm_sales_details FROM 'E:\DataEngineering Projects\DataWareHouse\dataset\source_crm\sales_details.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

			SET @end_time=GETDATE();
			PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> -----------------';


			SET @start_time=GETDATE();

			PRINT '>> TRUNCATING TABLE : BRONZE.ERP_LOC_A101';
			TRUNCATE TABLE bronze.erp_loc_a101;
			PRINT '>> INSERTING DATA INTO : BRONZE.ERP_LOC_A101';
			BULK INSERT bronze.erp_loc_a101 FROM 'E:\DataEngineering Projects\DataWareHouse\dataset\source_erp\LOC_A101.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

			SET @end_time=GETDATE();
			PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> -----------------';

			SET @start_time=GETDATE();

			PRINT '>> TRUNCATING TABLE : BRONZE.ERP_CUST_AZ12';
			TRUNCATE TABLE bronze.erp_cust_az12;
			PRINT '>> INSERTING DATA INTO : BRONZE.ERP_CUST_AZ12';
			BULK INSERT bronze.erp_cust_az12 FROM 'E:\DataEngineering Projects\DataWareHouse\dataset\source_erp\CUST_AZ12.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

			SET @end_time=GETDATE();
			PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> -----------------';

			SET @start_time=GETDATE();

			PRINT '>> TRUNCATING TABLE : BRONZE.ERP_PX_CAT_G1V2';
			TRUNCATE TABLE bronze.erp_px_cat_g1v2;
			PRINT '>> INSERTING DATA INTO : BRONZE.ERP_PX_CAT_G1V2';
			BULK INSERT bronze.erp_px_cat_g1v2 FROM 'E:\DataEngineering Projects\DataWareHouse\dataset\source_erp\PX_CAT_G1V2.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

			SET @end_time=GETDATE();
			PRINT '>> LOAD DURATION : ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
			PRINT '>> -----------------';



			SET @batch_end_time=GETDATE();

			PRINT '======================================================================';

			PRINT 'LOADING BRONZE LAYER IS COMPLETED';
			PRINT 'TOTAL LOAD DURATION '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '======================================================================';

			END TRY 
			BEGIN CATCH
				PRINT '=================================================';
				PRINT ' ERROR OCCURED DURING LOADING BRONZE LAYER';
				PRINT 'ERROR MESSAGE '+ERROR_MESSAGE();
				PRINT 'ERROR NUMBER '+CAST (ERROR_NUMBER() AS NVARCHAR);
				PRINT 'ERROR STATE '+CAST (ERROR_STATE() AS NVARCHAR);
				PRINT '=================================================';
			END CATCH
END





