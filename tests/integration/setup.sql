-- dbkrab Integration Test Setup Script
-- Creates test database, tables, and enables CDC

USE master;
GO

-- Create test database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'dbkrab_test')
BEGIN
    CREATE DATABASE dbkrab_test;
END
GO

USE dbkrab_test;
GO

-- Create test user
IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'dbkrab_test_user')
BEGIN
    CREATE LOGIN dbkrab_test_user WITH PASSWORD = 'Test1234!';
    CREATE USER dbkrab_test_user FOR LOGIN dbkrab_test_user;
    ALTER ROLE db_datareader ADD MEMBER dbkrab_test_user;
    ALTER ROLE db_datawriter ADD MEMBER dbkrab_test_user;
END
GO

-- Create test tables
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TestProducts')
BEGIN
    CREATE TABLE TestProducts (
        ProductID INT IDENTITY(1,1) PRIMARY KEY,
        ProductName NVARCHAR(100) NOT NULL,
        Price DECIMAL(10,2) NOT NULL,
        Stock INT NOT NULL DEFAULT 0,
        CreatedAt DATETIME2 DEFAULT SYSDATETIME()
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TestOrders')
BEGIN
    CREATE TABLE TestOrders (
        OrderID INT IDENTITY(1,1) PRIMARY KEY,
        ProductID INT NOT NULL FOREIGN KEY REFERENCES TestProducts(ProductID),
        Quantity INT NOT NULL,
        TotalAmount DECIMAL(10,2) NOT NULL,
        OrderDate DATETIME2 DEFAULT SYSDATETIME()
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TestOrderItems')
BEGIN
    CREATE TABLE TestOrderItems (
        ItemID INT IDENTITY(1,1) PRIMARY KEY,
        OrderID INT NOT NULL FOREIGN KEY REFERENCES TestOrders(OrderID),
        ItemName NVARCHAR(100) NOT NULL,
        ItemPrice DECIMAL(10,2) NOT NULL
    );
END
GO

-- Enable CDC on database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'dbkrab_test' AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

-- Enable CDC on test tables
IF NOT EXISTS (SELECT * FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.TestProducts'))
BEGIN
    EXEC sys.sp_cdc_enable_table 
        @source_schema = N'dbo',
        @source_name = N'TestProducts',
        @role_name = NULL,
        @supports_net_changes = 1;
END
GO

IF NOT EXISTS (SELECT * FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.TestOrders'))
BEGIN
    EXEC sys.sp_cdc_enable_table 
        @source_schema = N'dbo',
        @source_name = N'TestOrders',
        @role_name = NULL,
        @supports_net_changes = 1;
END
GO

IF NOT EXISTS (SELECT * FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('dbo.TestOrderItems'))
BEGIN
    EXEC sys.sp_cdc_enable_table 
        @source_schema = N'dbo',
        @source_name = N'TestOrderItems',
        @role_name = NULL,
        @supports_net_changes = 1;
END
GO

-- Insert initial test data
INSERT INTO TestProducts (ProductName, Price, Stock) VALUES
    ('Test Product A', 10.00, 100),
    ('Test Product B', 20.00, 50),
    ('Test Product C', 30.00, 25);
GO

-- Grant CDC schema permissions to test user
GRANT SELECT ON SCHEMA::cdc TO dbkrab_test_user;
GO

PRINT 'CDC setup completed successfully!';
GO
