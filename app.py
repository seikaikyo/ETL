#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import datetime
import sys
import os
import pandas as pd
from sqlalchemy import text
from typing import Dict, Any, Optional

# 導入自定義模組
from config import get_config_manager, get_etl_config
from database import DatabaseManager
from sql_loader import SQLLoader


def setup_logging(debug: bool = False) -> logging.Logger:
    """設定日誌記錄器"""
    etl_config = get_etl_config()
    level = logging.DEBUG if debug else getattr(logging, etl_config.LOG_LEVEL)
    
    logging.basicConfig(
        level=level,
        format=etl_config.LOG_FORMAT
    )
    logger = logging.getLogger("ETL_Process")
    
    if debug:
        logger.debug("已啟用詳細除錯模式")
    
    return logger


class ETLProcessor:
    """ETL處理器 - 負責核心的ETL邏輯"""
    
    def __init__(self, config_manager, db_manager: DatabaseManager, sql_loader: SQLLoader, logger: logging.Logger):
        self.config_manager = config_manager
        self.db_manager = db_manager
        self.sql_loader = sql_loader
        self.logger = logger
        self.etl_config = config_manager.etl_config
    
    def ensure_etl_summary_table(self, target_db: str = 'tableau_db'):
        """確保 ETL_SUMMARY 表存在且結構正確"""
        table_name = self.etl_config.ETL_SUMMARY_TABLE
        sql = f"""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
        BEGIN
            CREATE TABLE {table_name} (
                id INT IDENTITY(1,1) PRIMARY KEY,
                [TIMESTAMP] DATETIME DEFAULT GETDATE(),
                [SOURCE_TYPE] NVARCHAR(50),
                [QUERY_NAME] NVARCHAR(255),
                [TARGET_TABLE] NVARCHAR(255),
                [ROW_COUNT] INT,
                [ETL_DATE] DATETIME DEFAULT GETDATE(),
                [SUMMARY_TYPE] NVARCHAR(50) NULL,
                [ETL_STATUS] NVARCHAR(50) NULL,
                [mes_status] NVARCHAR(50) NULL,
                [sap_status] NVARCHAR(50) NULL,
                [mes_rows] INT NULL,
                [sap_rows] INT NULL
            )
        END
        ELSE
        BEGIN
            -- 確保新增欄位存在
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                           WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'SUMMARY_TYPE')
            BEGIN
                ALTER TABLE {table_name} ADD [SUMMARY_TYPE] NVARCHAR(50) NULL
            END
            
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                           WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'ETL_STATUS')
            BEGIN
                ALTER TABLE {table_name} ADD [ETL_STATUS] NVARCHAR(50) NULL
            END
            
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                           WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'mes_status')
            BEGIN
                ALTER TABLE {table_name} ADD [mes_status] NVARCHAR(50) NULL
            END
            
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                           WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'sap_status')
            BEGIN
                ALTER TABLE {table_name} ADD [sap_status] NVARCHAR(50) NULL
            END
            
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                           WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'mes_rows')
            BEGIN
                ALTER TABLE {table_name} ADD [mes_rows] INT NULL
            END
            
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                           WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = 'sap_rows')
            BEGIN
                ALTER TABLE {table_name} ADD [sap_rows] INT NULL
            END
        END
        """
        try:
            with self.db_manager.get_engine_context(target_db) as engine:
                with engine.begin() as conn:
                    conn.execute(text(sql))
            self.logger.info(f"已確保 {table_name} 表存在且結構正確")
        except Exception as e:
            self.logger.warning(f"檢查或創建 {table_name} 表失敗: {e}")
    
    def backup_and_truncate(self, target_db: str, table_name: str) -> Optional[str]:
        """備份並清空目標表"""
        # 先檢查表是否存在
        if not self.db_manager.check_table_exists(target_db, table_name):
            self.logger.info(f"表 {table_name} 不存在，將創建新表")
            return None  # 不執行備份和清空，返回None表示沒有執行備份

        # 表存在，進行備份和清空
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        backup_name = f"{table_name}_backup_{timestamp}"
        
        try:
            with self.db_manager.get_engine_context(target_db) as engine:
                with engine.begin() as conn:
                    # 使用參數化查詢進行備份
                    backup_sql = f"SELECT * INTO {backup_name} FROM {table_name}"
                    conn.execute(text(backup_sql))
                    
                    # 清空目標表
                    truncate_sql = f"TRUNCATE TABLE {table_name}"
                    conn.execute(text(truncate_sql))
            
            self.logger.info(f"已備份 {table_name} 至 {backup_name} 並清空目標表")
            return backup_name
            
        except Exception as e:
            self.logger.warning(f"備份或清空表 {table_name} 失敗: {e}")
            return None
    
    def run_etl(self, query: Dict[str, Any], source_db: str, target_db: str) -> int:
        """
        執行單筆 ETL - 使用分批處理避免參數過多錯誤，並處理NULL值
        
        Args:
            query: 查詢配置
            source_db: 來源資料庫名稱
            target_db: 目標資料庫名稱
            
        Returns:
            處理的資料筆數
        """
        name = query['name']
        target_table = query['target_table']
        sql_file = query['sql_file']
        source_type = name.split('_')[0].upper()  # 從查詢名稱取得來源類型 (MES或SAP)

        # 從SQL文件讀取SQL語句
        sql = self.sql_loader.load_sql_file(sql_file)

        self.logger.info(f"處理查詢: {name}")
        try:
            with self.db_manager.get_connection_context(source_db) as src_conn:
                df = pd.read_sql(sql, src_conn)

            # 處理NULL值
            if not df.empty:
                # 記錄NULL值情況
                null_counts = df.isnull().sum().sum()
                if null_counts > 0:
                    self.logger.warning(f"查詢 {name} 包含 {null_counts} 個NULL值")

                # 針對數值型欄位，將NULL填充為0
                numeric_columns = df.select_dtypes(
                    include=['int', 'float']).columns
                df[numeric_columns] = df[numeric_columns].fillna(0)

                # 針對字串型欄位，將NULL填充為空字串
                string_columns = df.select_dtypes(include=['object']).columns
                df[string_columns] = df[string_columns].fillna('')

            self.logger.info(f"讀取 {len(df)} 筆資料，處理後資料品質正常")
        except Exception as e:
            self.logger.error(f"執行查詢 {name} 失敗: {e}")
            raise

        if df.empty:
            self.logger.warning(f"查詢 {name} 未返回任何資料")
            # 記錄ETL執行結果
            self._record_query_result(target_db, source_type, name, target_table, 0)
            return 0

        # 備份和清空表 (如果表存在)
        backup_name = self.backup_and_truncate(target_db, target_table)
        if backup_name:
            self.logger.info(f"備份 {target_table} 至 {backup_name}，並清空目標表")

        # 分批處理
        batch_size = self.etl_config.BATCH_SIZE
        total_rows = len(df)
        processed = 0

        try:
            with self.db_manager.get_engine_context(target_db) as tgt_engine:
                # 使用if_exists='replace'來處理表不存在的情況
                for i in range(0, total_rows, batch_size):
                    chunk = df.iloc[i:min(i+batch_size, total_rows)]
                    # 首次迭代使用replace，後續使用append
                    mode = 'replace' if i == 0 else 'append'
                    chunk.to_sql(target_table, tgt_engine, if_exists=mode,
                                 index=False, method=None)
                    processed += len(chunk)
                    if processed % self.etl_config.PROGRESS_REPORT_INTERVAL == 0 or processed == total_rows:
                        progress_pct = int(processed/total_rows*100)
                        self.logger.info(f"進度: {processed}/{total_rows} 筆 ({progress_pct}%)")

                self.logger.info(f"已匯入總計 {total_rows} 筆至 {target_table}")

            # 記錄ETL執行結果
            self._record_query_result(target_db, source_type, name, target_table, total_rows)
            return total_rows
            
        except Exception as e:
            self.logger.error(f"匯入資料至 {target_table} 失敗: {e}")

            # 還原備份 (如果有)
            if backup_name:
                self._restore_from_backup(target_db, target_table, backup_name)
            raise

    def _record_query_result(self, target_db: str, source_type: str, query_name: str, target_table: str, row_count: int):
        """記錄單個查詢的執行結果"""
        try:
            table_name = self.etl_config.ETL_SUMMARY_TABLE
            with self.db_manager.get_engine_context(target_db) as engine:
                with engine.begin() as conn:
                    stmt = text(
                        f"INSERT INTO {table_name} ([TIMESTAMP], [SOURCE_TYPE], [QUERY_NAME], [TARGET_TABLE], [ROW_COUNT], [ETL_DATE], [SUMMARY_TYPE])"
                        " VALUES (GETDATE(), :source_type, :query_name, :target_table, :row_count, GETDATE(), 'QUERY')"
                    )
                    params = {
                        'source_type': source_type,
                        'query_name': query_name,
                        'target_table': target_table,
                        'row_count': row_count
                    }
                    conn.execute(stmt, params)
        except Exception as e:
            self.logger.warning(f"記錄查詢執行結果失敗: {e}")
    
    def _restore_from_backup(self, target_db: str, table_name: str, backup_name: str):
        """從備份還原表"""
        try:
            with self.db_manager.get_engine_context(target_db) as engine:
                with engine.begin() as conn:
                    # 檢查目標表是否存在，如果存在則刪除
                    if self.db_manager.check_table_exists(target_db, table_name):
                        conn.execute(text(f"DROP TABLE {table_name}"))
                    
                    # 從備份還原
                    conn.execute(text(f"SELECT * INTO {table_name} FROM {backup_name}"))
            
            self.logger.info(f"已還原 {table_name} 從備份 {backup_name}")
        except Exception as restore_err:
            self.logger.error(f"還原備份失敗: {restore_err}")
    
    def record_etl_summary(self, target_db: str, mes_status: str, sap_status: str, mes_rows: int, sap_rows: int):
        """記錄整體ETL執行摘要"""
        try:
            table_name = self.etl_config.ETL_SUMMARY_TABLE
            with self.db_manager.get_engine_context(target_db) as engine:
                with engine.begin() as conn:
                    stmt = text(
                        f"INSERT INTO {table_name} ([TIMESTAMP], [SOURCE_TYPE], [QUERY_NAME], [TARGET_TABLE], [ROW_COUNT], [ETL_DATE], "
                        "[SUMMARY_TYPE], [ETL_STATUS], [mes_status], [sap_status], [mes_rows], [sap_rows])"
                        " VALUES (GETDATE(), 'ALL', 'ETL_COMPLETE', 'ALL_TABLES', :total_rows, GETDATE(), "
                        "'SUMMARY', 'COMPLETE', :mes_status, :sap_status, :mes_rows, :sap_rows)"
                    )
                    params = {
                        'total_rows': mes_rows + sap_rows,
                        'mes_status': mes_status,
                        'sap_status': sap_status,
                        'mes_rows': mes_rows,
                        'sap_rows': sap_rows
                    }
                    conn.execute(stmt, params)
            self.logger.info("已記錄ETL執行摘要")
        except Exception as e:
            self.logger.warning(f"記錄ETL執行摘要失敗: {e}")
    
    def run_queries(self, queries: list, source_db: str, target_db: str) -> tuple:
        """
        執行一組查詢
        
        Returns:
            (status, total_rows): 狀態和總記錄數
        """
        total_rows = 0
        status = '成功'
        
        try:
            for query in queries:
                self.logger.debug(f"執行查詢: {query['name']}")
                rows = self.run_etl(query, source_db, target_db)
                total_rows += rows
            
            self.logger.info(f"已完成所有查詢，處理 {total_rows} 筆資料")
            
        except Exception as e:
            self.logger.error(f"執行查詢失敗: {e}")
            status = '失敗'
        
        return status, total_rows


def main():
    """主程式入口"""
    # 解析命令列參數
    parser = argparse.ArgumentParser(description='ETL 系統 - 從 MES 和 SAP 數據庫提取數據到 Tableau')
    parser.add_argument('--all', action='store_true', help='執行所有ETL')
    parser.add_argument('--mes', action='store_true', help='只執行 MES ETL')
    parser.add_argument('--sap', action='store_true', help='只執行 SAP ETL')
    parser.add_argument('--debug', action='store_true', help='啟用詳細的除錯訊息')
    parser.add_argument('--config', help='指定配置檔案路徑', default='db.json')
    args = parser.parse_args()
    
    # 設定日誌
    logger = setup_logging(args.debug)
    
    # 初始化配置管理器
    try:
        config_manager = get_config_manager()
        if args.config != 'db.json':
            config_manager.db_config_file = args.config
        
        # 驗證配置
        if not config_manager.validate_config():
            logger.error("配置驗證失敗，終止程式")
            sys.exit(1)
    except Exception as e:
        logger.error(f"初始化配置失敗: {e}")
        sys.exit(1)
    
    # 初始化資料庫管理器
    db_manager = DatabaseManager(config_manager)
    sql_loader = SQLLoader(config_manager)
    etl_processor = ETLProcessor(config_manager, db_manager, sql_loader, logger)
    
    logger.info('='*60)
    logger.info(f"ETL 程序啟動 - {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
    
    try:
        # 確保 ETL_SUMMARY 表結構正確
        etl_processor.ensure_etl_summary_table('tableau_db')
        
        # 載入查詢定義
        query_metadata = config_manager.load_query_metadata()
        mes_queries = config_manager.get_queries_by_type('mes')
        sap_queries = config_manager.get_queries_by_type('sap')
        logger.info(f"已載入查詢定義: MES={len(mes_queries)}個, SAP={len(sap_queries)}個")
        
        mes_status = sap_status = '跳過'
        mes_rows = sap_rows = 0
        
        # 執行 MES ETL
        if args.all or args.mes:
            if db_manager.test_connection('mes_db'):
                logger.info('開始 MES ETL 流程...')
                mes_status, mes_rows = etl_processor.run_queries(mes_queries, 'mes_db', 'tableau_db')
                logger.info(f"MES ETL 完成，處理 {mes_rows} 筆資料")
            else:
                logger.error("無法執行 MES ETL: MES 資料庫連線失敗")
                mes_status = '失敗'
        
        # 執行 SAP ETL
        if args.all or args.sap:
            if db_manager.test_connection('sap_db'):
                logger.info('開始 SAP ETL 流程...')
                sap_status, sap_rows = etl_processor.run_queries(sap_queries, 'sap_db', 'tableau_db')
                logger.info(f"SAP ETL 完成，處理 {sap_rows} 筆資料")
            else:
                logger.error("無法執行 SAP ETL: SAP 資料庫連線失敗")
                sap_status = '失敗'
        
        logger.info('='*60)
        logger.info(f"ETL 執行結果 - MES: {mes_status} ({mes_rows}筆), SAP: {sap_status} ({sap_rows}筆)")
        
        # 記錄整體ETL執行摘要
        etl_processor.record_etl_summary('tableau_db', mes_status, sap_status, mes_rows, sap_rows)
        
        # 檢查是否有失敗
        if mes_status == '失敗' or sap_status == '失敗':
            logger.error('ETL 處理有部分失敗！')
            sys.exit(1)
        
        logger.info('ETL 處理完成')
        
    except Exception as e:
        logger.error(f"ETL 程序執行失敗: {e}")
        sys.exit(1)
    
    finally:
        # 清理資源
        db_manager.close_connections()
        sql_loader.clear_cache()


if __name__ == '__main__':
    main()