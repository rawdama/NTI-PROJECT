/*
=========================================================
 Script:     Data Warehouse Initialization
 Purpose:    This script creates a new database named 'DataWarehouse'
             and defines a multi-layered schema structure based on the 
             medallion architecture:
             
             - Bronze: Raw data ingestion
             - Silver: Cleaned and transformed data
             - Gold: Business-ready data for reporting and analytics
             
             This structure helps maintain a clean, organized, and scalable 
             data pipeline in a data warehouse environment.
=========================================================
*/


-- Create the main database for the Data Warehouse
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the newly created database
USE DataWarehouse;
GO

-- Create the 'bronze' schema
-- This layer is typically used to store raw or minimally processed data
CREATE SCHEMA bronze;
GO

-- Create the 'silver' schema
-- This layer stores cleaned, enriched, or lightly transformed data
CREATE SCHEMA silver;
GO

-- Create the 'gold' schema
-- This layer contains business-ready data for reporting and analytics
CREATE SCHEMA gold;
GO
