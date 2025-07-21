/*
============================================================================================
Create bronze tables
============================================================================================
Script purpose:
      This script create tables in bronze schema , dropping them if they are already exists
===========================================================================================

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze as 
Begin
 DECLARE @start_time DATETIME, @end_time DATETIME ,@batch_start_time DATETIME,@batch_end_time DATETIME;
 BEGIN TRY
		set @batch_start_time=GETDATE();
		PRINT'====================================================';
		PRINT'Loading Bronze Layer';
		PRINT'====================================================';

		PRINT'----------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'----------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT'>> Turncating Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>> Insering Data Into Table : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Smart Store\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'---------------------'

		SET @start_time=GETDATE();
		PRINT'>> Turncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'>> Insering Data Into Table : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Smart Store\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'---------------------'

		SET @start_time=GETDATE();
		PRINT'>> Turncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'>> Insering Data Into Table : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Smart Store\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'---------------------'

		PRINT'----------------------------------------------------';
		PRINT'Loading ERP Tables';
		PRINT'----------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT'>> Turncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT'>> Insering Data Into Table : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Smart Store\Documents\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'---------------------'

		SET @start_time=GETDATE();
		PRINT'>> Turncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT'>> Insering Data Into Table : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Smart Store\Documents\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'---------------------'


		SET @start_time=GETDATE();
		PRINT'>> Turncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT'>> Insering Data Into Table : bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Smart Store\Documents\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT'>> Load Duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT'---------------------'
		SET @batch_end_time=GETDATE();
		PRINT'=============================================';
		PRINT'Loading bronze layer is completed';
		PRINT'>> Total  Duration: '+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR)+' seconds';
		PRINT'=============================================';


	END TRY
	BEGIN CATCH
		PRINT'=============================================='
		PRINT' ocurred during loading bronze layer'
		PRINT'Error Message'+ERROR_MESSAGE();
		PRINT'Error Number'+CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT'=============================================='
	END CATCH
END

EXEC bronze.load_bronze
