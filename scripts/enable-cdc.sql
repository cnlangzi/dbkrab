-- Enable CDC for dbkrab tracked tables
-- Run this script with a user that has db_owner role on the F9All database
-- Usage: sqlcmd -S 172.16.16.34 -U <dba_user> -P <password> -d F9All -i enable-cdc.sql

USE F9All;
GO

-- Check if CDC is enabled at database level
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
BEGIN
    PRINT 'Enabling CDC at database level...';
    EXEC sp_cdc_enable_db;
    PRINT 'CDC enabled at database level.';
END
ELSE
BEGIN
    PRINT 'CDC is already enabled at database level.';
END
GO

-- Enable CDC for dbo.cost table
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_cost')
BEGIN
    PRINT 'Enabling CDC for dbo.cost...';
    EXEC sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name = 'cost',
        @role_name = NULL,
        @supports_net_changes = 0;
    PRINT 'CDC enabled for dbo.cost.';
END
ELSE
BEGIN
    PRINT 'CDC is already enabled for dbo.cost.';
END
GO

-- Enable CDC for dbo.costitem table
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_costitem')
BEGIN
    PRINT 'Enabling CDC for dbo.costitem...';
    EXEC sp_cdc_enable_table
        @source_schema = 'dbo',
        @source_name = 'costitem',
        @role_name = NULL,
        @supports_net_changes = 0;
    PRINT 'CDC enabled for dbo.costitem.';
END
ELSE
BEGIN
    PRINT 'CDC is already enabled for dbo.costitem.';
END
GO

-- Verify CDC status
PRINT 'CDC Status Summary:';
SELECT 
    s.name AS schema_name,
    t.name AS table_name,
    c.capture_instance,
    'ENABLED' AS cdc_status
FROM cdc.change_tables c
JOIN sys.tables t ON c.source_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE c.capture_instance IN ('dbo_cost', 'dbo_costitem', 'dbo_cdc_costitem')
ORDER BY s.name, t.name;
GO

PRINT 'CDC setup complete!';
GO
