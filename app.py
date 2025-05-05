#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import pyodbc
import sys
import logging
import pandas as pd
from datetime import datetime

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_log.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ETL_Test")


def load_db_config():
    """從 db.json 載入資料庫配置"""
    try:
        with open('db.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"讀取配置文件時出錯: {e}")
        sys.exit(1)


def get_connection_string(db_config):
    """建立連接字串"""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['username']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes;"
    )
    return conn_str


def test_db_connection(db_type, db_config):
    """測試資料庫連接"""
    try:
        logger.info(f"開始測試 {db_type} 資料庫連接...")

        conn_str = get_connection_string(db_config)

        logger.info(
            f"嘗試連接到 {db_type} 資料庫 ({db_config['server']}/{db_config['database']})...")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 執行簡單查詢來驗證連接
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()

        if row:
            logger.info(f"成功連接到 {db_type} 資料庫！")
            logger.info(f"SQL Server 版本: {row[0]}")

            # 獲取資料表列表
            cursor.execute(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
            tables = cursor.fetchall()

            if tables:
                logger.info(f"資料庫中的資料表數量: {len(tables)}")
                logger.info("資料表列表 (前 5 個):")
                for i, table in enumerate(tables[:5]):
                    logger.info(f"  - {table[0]}")
                if len(tables) > 5:
                    logger.info(f"  ... 共 {len(tables)} 個資料表")
            else:
                logger.info("資料庫中沒有找到資料表")

            # 檢查連接時間
            cursor.execute("SELECT GETDATE()")
            db_time = cursor.fetchone()[0]
            logger.info(f"資料庫當前時間: {db_time}")

        cursor.close()
        conn.close()
        logger.info(f"{db_type} 資料庫連接測試完成，連接已關閉。")
        return True

    except Exception as e:
        logger.error(f"連接 {db_type} 資料庫時出錯: {e}")
        return False


def test_simple_etl():
    """測試簡單的 ETL 操作"""
    try:
        logger.info("開始測試簡單 ETL 操作...")
        config = load_db_config()
        source_config = config["source_db"]
        target_config = config["target_db"]

        # 連接來源資料庫
        source_conn_str = get_connection_string(source_config)
        source_conn = pyodbc.connect(source_conn_str)

        # 執行查詢，獲取測試資料 (限制僅 5 筆)
        query = """
        SELECT TOP 5 TABLE_NAME, TABLE_SCHEMA
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE='BASE TABLE'
        """

        logger.info("從來源資料庫讀取資料...")
        df = pd.read_sql(query, source_conn)

        logger.info(f"成功讀取 {len(df)} 筆資料")
        logger.info(f"資料樣本: \n{df.head().to_string()}")

        # 關閉來源連接
        source_conn.close()

        # 測試是否能連接到目標資料庫
        target_conn_str = get_connection_string(target_config)
        target_conn = pyodbc.connect(target_conn_str)

        logger.info("ETL 測試成功 - 可以從來源讀取資料並連接到目標資料庫")

        # 關閉目標連接
        target_conn.close()

        return True

    except Exception as e:
        logger.error(f"執行 ETL 測試時出錯: {e}")
        return False


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info(f"ETL 測試程序啟動 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 載入資料庫配置
    config = load_db_config()

    # 測試來源資料庫連接
    source_success = test_db_connection("來源(MES)", config["source_db"])

    # 測試目標資料庫連接
    target_success = test_db_connection("目標(Tableau)", config["target_db"])

    # 若兩個資料庫都連接成功，則測試簡單 ETL
    if source_success and target_success:
        logger.info("兩個資料庫連接測試都成功，開始測試簡單的 ETL 操作...")
        etl_success = test_simple_etl()
    else:
        etl_success = False

    # 總結測試結果
    logger.info("=" * 50)
    logger.info("測試結果摘要:")
    logger.info(f"來源資料庫(MES)連接: {'成功' if source_success else '失敗'}")
    logger.info(f"目標資料庫(Tableau)連接: {'成功' if target_success else '失敗'}")

    if source_success and target_success:
        logger.info(f"簡單 ETL 測試: {'成功' if etl_success else '失敗'}")

    if source_success and target_success and etl_success:
        logger.info("ETL 環境測試全部成功！")
        sys.exit(0)
    else:
        logger.error("ETL 環境測試失敗！")
        sys.exit(1)
