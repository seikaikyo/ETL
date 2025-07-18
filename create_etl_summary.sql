-- 建立或修正 ETL_SUMMARY 表結構
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ETL_SUMMARY')
BEGIN
    CREATE TABLE ETL_SUMMARY (
        id INT IDENTITY(1,1) PRIMARY KEY,
        run_time DATETIME,
        mes_status NVARCHAR(50),
        sap_status NVARCHAR(50),
        mes_rows INT,
        sap_rows INT
    )
    PRINT 'ETL_SUMMARY 表已建立'
END
ELSE
BEGIN
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'run_time')
    BEGIN
        ALTER TABLE ETL_SUMMARY ADD run_time DATETIME
        PRINT '已新增 run_time 欄位'
    END

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'mes_status')
    BEGIN
        ALTER TABLE ETL_SUMMARY ADD mes_status NVARCHAR(50)
        PRINT '已新增 mes_status 欄位'
    END

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'sap_status')
    BEGIN
        ALTER TABLE ETL_SUMMARY ADD sap_status NVARCHAR(50)
        PRINT '已新增 sap_status 欄位'
    END

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'mes_rows')
    BEGIN
        ALTER TABLE ETL_SUMMARY ADD mes_rows INT
        PRINT '已新增 mes_rows 欄位'
    END

    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                 WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'sap_rows')
    BEGIN
        ALTER TABLE ETL_SUMMARY ADD sap_rows INT
        PRINT '已新增 sap_rows 欄位'
    END
END