#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import pyodbc
import sys
import logging
import pandas as pd
import os
from datetime import datetime
import time
import argparse

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_log.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ETL_Process")


def load_db_config():
    """從 db.json 載入資料庫配置"""
    try:
        with open('db.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"讀取配置文件時出錯: {e}")
        sys.exit(1)


def load_query_config(file_path):
    """從 query JSON 檔案載入 SQL 查詢配置"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"讀取查詢配置文件時出錯: {e}")
        return None


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


def execute_etl(source_type, query_file):
    """執行 ETL 處理"""
    try:
        # 載入配置
        logger.info(f"開始執行 {source_type} ETL 處理...")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        config = load_db_config()

        if source_type.lower() == "mes":
            source_config = config["mes_db"]
        elif source_type.lower() == "sap":
            source_config = config["sap_db"]
        else:
            logger.error(f"不支援的來源類型: {source_type}")
            return False

        target_config = config["tableau_db"]

        # 載入查詢配置
        query_config = load_query_config(query_file)
        if not query_config:
            logger.error(f"無法載入查詢配置: {query_file}")
            return False

        # 連接來源資料庫
        source_conn_str = get_connection_string(source_config)
        source_conn = pyodbc.connect(source_conn_str)

        # 連接目標資料庫
        target_conn_str = get_connection_string(target_config)
        target_conn = pyodbc.connect(target_conn_str)
        target_cursor = target_conn.cursor()

        # 處理每個查詢
        for query_item in query_config["queries"]:
            query_name = query_item["name"]
            target_table = query_item["target_table"]
            sql = query_item["sql"]

            logger.info(f"處理查詢: {query_name}")
            logger.info(f"目標資料表: {target_table}")

            # 讀取來源資料
            logger.info(f"從 {source_type} 資料庫讀取資料...")
            df = pd.read_sql(sql, source_conn)
            row_count = len(df)
            logger.info(f"成功讀取 {row_count} 筆資料")

            if row_count > 0:
                # 新增時間戳記欄位
                df['ETL_TIMESTAMP'] = timestamp
                df['ETL_SOURCE'] = source_type

                # 先檢查目標資料表是否存在
                target_cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                               WHERE TABLE_NAME = '{target_table}')
                BEGIN
                    SELECT 'Table does not exist'
                END
                ELSE
                BEGIN
                    SELECT 'Table exists'
                END
                """)
                table_exists = target_cursor.fetchone()[0] == 'Table exists'

                if not table_exists:
                    logger.info(f"目標資料表 {target_table} 不存在，建立新資料表...")
                    # 從 DataFrame 建立資料表欄位定義
                    columns = []
                    for col_name, dtype in zip(df.columns, df.dtypes):
                        sql_type = "VARCHAR(MAX)"  # 預設類型
                        if "int" in str(dtype):
                            sql_type = "INT"
                        elif "float" in str(dtype):
                            sql_type = "FLOAT"
                        elif "datetime" in str(dtype):
                            sql_type = "DATETIME"
                        columns.append(f"[{col_name}] {sql_type}")

                    create_table_sql = f"CREATE TABLE {target_table} (\n"
                    create_table_sql += ",\n".join(columns)
                    create_table_sql += "\n)"

                    target_cursor.execute(create_table_sql)
                    target_conn.commit()
                    logger.info(f"成功建立資料表 {target_table}")

                # 將資料匯入目標資料表
                logger.info(f"將資料匯入目標資料表 {target_table}...")

                # 備份已存在的資料
                if table_exists:
                    backup_table = f"{target_table}_backup_{timestamp}"
                    logger.info(f"備份現有資料到 {backup_table}...")
                    target_cursor.execute(
                        f"SELECT * INTO {backup_table} FROM {target_table}")
                    target_conn.commit()

                    # 清空目標資料表
                    logger.info(f"清空目標資料表 {target_table}...")
                    target_cursor.execute(f"TRUNCATE TABLE {target_table}")
                    target_conn.commit()

                # 逐行插入資料
                inserted_count = 0
                for _, row in df.iterrows():
                    columns = [f"[{col}]" for col in df.columns]
                    placeholders = ["?" for _ in df.columns]

                    insert_sql = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                    target_cursor.execute(insert_sql, *row)
                    inserted_count += 1

                    # 每隔 1000 筆提交一次
                    if inserted_count % 1000 == 0:
                        target_conn.commit()
                        logger.info(f"已插入 {inserted_count} 筆資料...")

                # 提交剩餘的資料
                target_conn.commit()
                logger.info(f"成功將 {inserted_count} 筆資料匯入 {target_table}")

                # 建立匯入摘要存檔
                summary_table = "ETL_SUMMARY"
                logger.info(f"記錄 ETL 匯入摘要到 {summary_table}...")

                # 檢查摘要資料表是否存在
                target_cursor.execute(f"""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
                               WHERE TABLE_NAME = '{summary_table}')
                BEGIN
                    CREATE TABLE {summary_table} (
                        [ID] INT IDENTITY(1,1) PRIMARY KEY,
                        [TIMESTAMP] VARCHAR(20),
                        [SOURCE_TYPE] VARCHAR(50),
                        [QUERY_NAME] VARCHAR(100),
                        [TARGET_TABLE] VARCHAR(100),
                        [ROW_COUNT] INT,
                        [ETL_DATE] DATETIME DEFAULT GETDATE()
                    )
                END
                """)
                target_conn.commit()

                # 插入摘要記錄
                target_cursor.execute(f"""
                INSERT INTO {summary_table} 
                ([TIMESTAMP], [SOURCE_TYPE], [QUERY_NAME], [TARGET_TABLE], [ROW_COUNT])
                VALUES (?, ?, ?, ?, ?)
                """, timestamp, source_type, query_name, target_table, row_count)
                target_conn.commit()

                logger.info(f"ETL 摘要記錄已保存")
            else:
                logger.warning(f"查詢 {query_name} 未返回資料，跳過匯入")

        # 關閉連接
        source_conn.close()
        target_conn.close()

        logger.info(f"{source_type} ETL 處理完成")
        return True

    except Exception as e:
        logger.error(f"執行 {source_type} ETL 處理時出錯: {e}")
        return False


def parse_arguments():
    """解析命令列參數"""
    parser = argparse.ArgumentParser(description='ETL 處理程序')
    parser.add_argument('--test', action='store_true', help='僅測試資料庫連接')
    parser.add_argument('--mes', action='store_true', help='執行 MES ETL')
    parser.add_argument('--sap', action='store_true', help='執行 SAP ETL')
    parser.add_argument('--all', action='store_true', help='執行所有 ETL')
    return parser.parse_args()


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info(f"ETL 程序啟動 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 解析命令列參數
    args = parse_arguments()

    # 載入資料庫配置
    config = load_db_config()

    # 測試資料庫連接
    if args.test:
        # 測試 MES 資料庫連接 (來源)
        mes_success = test_db_connection("來源(MES)", config["mes_db"])

        # 測試 SAP 資料庫連接 (來源)
        if "sap_db" in config:
            sap_success = test_db_connection("來源(SAP)", config["sap_db"])
        else:
            logger.warning("找不到 SAP 資料庫設定，跳過 SAP 連接測試")
            sap_success = False

        # 測試 Tableau 資料庫連接 (目標)
        target_success = test_db_connection(
            "目標(Tableau)", config["tableau_db"])

        # 總結測試結果
        logger.info("=" * 50)
        logger.info("測試結果摘要:")
        logger.info(f"來源資料庫(MES)連接: {'成功' if mes_success else '失敗'}")

        if "sap_db" in config:
            logger.info(f"來源資料庫(SAP)連接: {'成功' if sap_success else '失敗'}")

        logger.info(f"目標資料庫(Tableau)連接: {'成功' if target_success else '失敗'}")

        # 判斷整體測試是否成功
        overall_success = mes_success and target_success
        if "sap_db" in config and args.sap:
            overall_success = overall_success and sap_success

        if overall_success:
            logger.info("ETL 環境測試全部成功！")
            sys.exit(0)
        else:
            logger.error("ETL 環境測試有部分失敗！")
            sys.exit(1)

    # 執行 ETL
    success = True

    if args.mes or args.all:
        logger.info("開始 MES ETL 流程...")
        mes_success = execute_etl("MES", "mes_queries.json")
        success = success and mes_success

    if args.sap or args.all:
        logger.info("開始 SAP ETL 流程...")
        sap_success = execute_etl("SAP", "sap_queries.json")
        success = success and sap_success

    # 總結 ETL 結果
    logger.info("=" * 50)
    logger.info("ETL 執行結果摘要:")

    if args.mes or args.all:
        logger.info(f"MES ETL: {'成功' if mes_success else '失敗'}")

    if args.sap or args.all:
        logger.info(f"SAP ETL: {'成功' if sap_success else '失敗'}")

    if success:
        logger.info("ETL 處理全部成功！")
        sys.exit(0)
    else:
        logger.error("ETL 處理有部分失敗！")
        sys.exit(1)
