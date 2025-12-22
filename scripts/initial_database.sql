/*
================================
Create Database and schemas
================================
Script Purpose:
	For creating new database name 'DataWarehouse', if exist, it will drop and recreate.
	The script will setup three schemas within this database: 'bronze', 'silver', and 'gold'
Warning:
	Proceed with caution as the script will drop the entire 'DataWarehouse' database if it exits.
	All data in the database will be permanently deleted.
*/

USE master;
Go

-- Drop and recreate the 'DataWarehouse' database
If EXISTS (Select 1 from sys.databases Where name = 'DataWarehouse')
Begin
	Alter DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
Go

-- Create the 'DataWarehouse' database
Create Database DataWarehouse;
Go

Use DataWarehouse;
Go

-- Create Schemas
Create Schema bronze;
Go

Create Schema silver;
Go

Create Schema gold;
Go
