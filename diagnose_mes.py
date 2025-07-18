#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import pyodbc
import sys
import logging
import pandas as pd
import os
from datetime import datetime

# 設定日誌
log_file = f"mes_diagnostic_{datetime.now().strftime('%Y%m%d%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("MES_Detailed_Diagnose")


def load_db_config():
    """從 db.json 載入資料庫配置"""
    try:
        with open('db.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"讀取配置文件時出錯: {e}")
        sys.exit(1)


def load_sql_file(sql_file):
    """從SQL檔案讀取查詢內容"""
    try:
        # 取得當前腳本所在目錄的絕對路徑
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # 拼接SQL文件的絕對路徑
        sql_path = os.path.join(base_dir, sql_file)

        with open(sql_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"讀取SQL文件失敗: {sql_file}, {e}")
        sys.exit(1)


def load_mes_queries():
    """載入 MES 查詢"""
    try:
        with open('query_metadata.json', 'r', encoding='utf-8') as f:
            queries = json.load(f)

        # 過濾出MES相關的查詢
        mes_queries = {
            "queries": [q for q in queries['queries'] if q['name'].startswith('mes_')]
        }

        # 讀取每個查詢的SQL
        for query in mes_queries['queries']:
            sql_file = query['sql_file']
            query['sql'] = load_sql_file(sql_file)

        return mes_queries
    except Exception as e:
        logger.error(f"讀取 MES 查詢文件時出錯: {e}")
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


def get_target_table_structure(conn, table_name):
    """獲取目標表的結構"""
    logger.info(f"獲取目標表結構: {table_name}")
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """)

        columns = []
        for row in cursor.fetchall():
            column_info = {
                "name": row[0],
                "type": row[1],
                "max_length": row[2],
                "precision": row[3],
                "scale": row[4],
                "nullable": row[5]
            }
            columns.append(column_info)

        return columns
    except Exception as e:
        logger.error(f"獲取目標表結構時出錯: {e}")
        return None


def check_data_for_issues(conn, query, batch_size=100):
    """分批檢查數據是否有問題"""
    logger.info(f"開始檢查數據問題，批次大小: {batch_size}")
    try:
        cursor = conn.cursor()

        # 修改查詢以包含行號
        count_query = f"SELECT COUNT(*) FROM ({query.replace(';', '')}) AS CountQuery"
        cursor.execute(count_query)
        total_rows = cursor.fetchone()[0]
        logger.info(f"總行數: {total_rows}")

        # 分批處理
        issue_found = False
        processed_rows = 0

        for offset in range(0, total_rows, batch_size):
            batch_query = f"""
            WITH NumberedRows AS (
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum,
                    *
                FROM ({query.replace(';', '')}) AS DataQuery
            )
            SELECT * FROM NumberedRows
            WHERE RowNum > {offset} AND RowNum <= {offset + batch_size}
            """

            try:
                logger.info(f"處理批次: {offset} 到 {offset + batch_size}")
                cursor.execute(batch_query)
                rows = cursor.fetchall()
                processed_rows += len(rows)
                logger.info(f"批次處理成功，處理 {len(rows)} 筆資料")
            except Exception as e:
                issue_found = True
                logger.error(f"批次 {offset} 到 {offset + batch_size} 處理時出錯: {e}")

                # 嘗試找出具體的問題行
                try:
                    logger.info("嘗試找出具體的問題行...")
                    for i in range(offset, offset + batch_size, 10):
                        small_batch_query = f"""
                        WITH NumberedRows AS (
                            SELECT 
                                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum,
                                *
                            FROM ({query.replace(';', '')}) AS DataQuery
                        )
                        SELECT * FROM NumberedRows
                        WHERE RowNum > {i} AND RowNum <= {i + 10}
                        """

                        try:
                            cursor.execute(small_batch_query)
                            rows = cursor.fetchall()
                            logger.info(
                                f"子批次 {i} 到 {i + 10} 處理成功，處理 {len(rows)} 筆資料")
                        except Exception as e2:
                            logger.error(f"子批次 {i} 到 {i + 10} 處理時出錯: {e2}")

                            # 嘗試逐行查詢
                            for j in range(i, i + 10):
                                row_query = f"""
                                WITH NumberedRows AS (
                                    SELECT 
                                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum,
                                        *
                                    FROM ({query.replace(';', '')}) AS DataQuery
                                )
                                SELECT * FROM NumberedRows
                                WHERE RowNum = {j}
                                """

                                try:
                                    cursor.execute(row_query)
                                    row = cursor.fetchone()
                                    if row:
                                        logger.info(f"行 {j} 處理成功")
                                except Exception as e3:
                                    logger.error(f"問題行 {j} 找到: {e3}")

                                    # 嘗試獲取該行的基本信息
                                    try:
                                        simple_query = f"""
                                        WITH NumberedRows AS (
                                            SELECT 
                                                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum,
                                                [MANUFACTURING_OD]
                                            FROM ({query.replace(';', '')}) AS DataQuery
                                        )
                                        SELECT * FROM NumberedRows
                                        WHERE RowNum = {j}
                                        """
                                        cursor.execute(simple_query)
                                        row = cursor.fetchone()
                                        if row:
                                            logger.error(
                                                f"問題行 {j} 的工單號: {row[1]}")
                                    except Exception:
                                        pass
                except Exception as e_inner:
                    logger.error(f"嘗試診斷問題行時出錯: {e_inner}")

            # 每處理10000行輸出一次進度
            if processed_rows % 10000 == 0 or (offset + batch_size) >= total_rows:
                progress = min(100, int(processed_rows * 100 / total_rows))
                logger.info(
                    f"處理進度: {progress}%, 已處理 {processed_rows} / {total_rows} 筆資料")

        if not issue_found:
            logger.info("所有數據處理成功，未發現問題")

        return not issue_found
    except Exception as e:
        logger.error(f"檢查數據問題時出錯: {e}")
        return False


def check_for_float_conversion_issues(conn, query):
    """檢查浮點數轉換問題"""
    logger.info("檢查可能存在浮點數轉換問題的記錄")
    try:
        # 針對 MES 損耗差異分析查詢的相關檢查
        if "損耗率" in query or "BOM" in query:
            # 檢查可能有問題的計算欄位
            check_query = """
            SELECT TOP 100
                mn.[MANUFACTURING_OD],
                bc.QTY,
                fm.TOTAL_IN_QTY,
                CASE
                    WHEN bc.QTY IS NULL OR bc.QTY = 0 THEN 'QTY為空或零'
                    ELSE 'OK'
                END AS QTY_STATUS,
                CASE
                    WHEN bc.QTY IS NOT NULL AND bc.QTY <> 0 AND
                         TRY_CAST((fm.TOTAL_IN_QTY - bc.QTY) / bc.QTY * 100 AS FLOAT) IS NULL
                    THEN '損耗率計算錯誤'
                    ELSE 'OK'
                END AS LOSS_CALC_STATUS
            FROM [MANUFACTURING_NO] mn
            LEFT JOIN (
                SELECT 
                    PLANT,
                    MANUFACTURING_OD,
                    MATERIAL,
                    OPERATION,
                    SUM(ISNULL(IN_QTY, 0)) AS TOTAL_IN_QTY
                FROM 
                    FEED_MATERIAL_DEVICE
                GROUP BY 
                    PLANT,
                    MANUFACTURING_OD,
                    MATERIAL,
                    OPERATION
            ) AS fm ON mn.[MANUFACTURING_OD] = fm.[MANUFACTURING_OD]
            LEFT JOIN [BOM_COMPONENT] bc ON 
                mn.[MANUFACTURING_OD] = bc.[BOM] AND 
                mn.[PLANT] = bc.[PLANT] AND 
                fm.[MATERIAL] = bc.[COMPONENT]
            WHERE 
                (bc.QTY IS NULL OR bc.QTY = 0) OR
                (bc.QTY IS NOT NULL AND bc.QTY <> 0 AND
                 TRY_CAST((fm.TOTAL_IN_QTY - bc.QTY) / bc.QTY * 100 AS FLOAT) IS NULL)
            ORDER BY mn.[MANUFACTURING_OD]
            """

            cursor = conn.cursor()
            cursor.execute(check_query)
            problem_rows = cursor.fetchall()

            if problem_rows:
                logger.error(f"找到 {len(problem_rows)} 筆可能有浮點數轉換問題的資料:")
                for i, row in enumerate(problem_rows):
                    logger.error(
                        f"問題記錄 {i+1}: 工單號={row[0]}, QTY={row[1]}, TOTAL_IN_QTY={row[2]}, QTY狀態={row[3]}, 損耗率計算狀態={row[4]}")
                return False
            else:
                logger.info("未發現浮點數轉換問題")
                return True

        return True  # 如果不是損耗差異分析，則直接返回True
    except Exception as e:
        logger.error(f"檢查浮點數轉換問題時出錯: {e}")
        return False


def diagnose_mes_queries():
    """診斷 MES 查詢"""
    logger.info("開始診斷 MES 查詢")

    # 載入配置
    config = load_db_config()
    mes_config = config["mes_db"]
    target_config = config["tableau_db"]

    # 載入 MES 查詢
    mes_queries = load_mes_queries()

    # 連接 MES 資料庫
    logger.info(f"連接 MES 資料庫: {mes_config['server']}/{mes_config['database']}")
    mes_conn_str = get_connection_string(mes_config)

    try:
        mes_conn = pyodbc.connect(mes_conn_str)
        logger.info("MES 資料庫連接成功")

        # 連接目標資料庫
        logger.info(
            f"連接目標資料庫: {target_config['server']}/{target_config['database']}")
        target_conn_str = get_connection_string(target_config)
        target_conn = pyodbc.connect(target_conn_str)
        logger.info("目標資料庫連接成功")

        # 診斷每個 MES 查詢
        for query_info in mes_queries["queries"]:
            query_name = query_info["name"]
            target_table = query_info["target_table"]
            sql = query_info["sql"]

            logger.info(f"開始診斷查詢: {query_name}, 目標表: {target_table}")

            # 1. 檢查目標表結構
            target_structure = get_target_table_structure(
                target_conn, target_table)
            if target_structure:
                logger.info(f"目標表 {target_table} 結構:")
                for col in target_structure:
                    logger.info(
                        f"  列名: {col['name']}, 類型: {col['type']}, 可空: {col['nullable']}")

            # 2. 檢查浮點數轉換問題
            float_check_result = check_for_float_conversion_issues(
                mes_conn, sql)
            if not float_check_result:
                logger.error(f"查詢 {query_name} 存在浮點數轉換問題")

            # 3. 分批檢查數據
            logger.info(f"開始分批檢查 {query_name} 查詢的數據")
            data_check_result = check_data_for_issues(mes_conn, sql)
            if not data_check_result:
                logger.error(f"查詢 {query_name} 在處理完整數據集時存在問題")

        # 關閉連接
        mes_conn.close()
        target_conn.close()
        logger.info("診斷完成，資料庫連接已關閉")

    except Exception as e:
        logger.error(f"診斷過程中出錯: {e}")
        sys.exit(1)

    # 顯示日誌文件位置
    logger.info(f"診斷日誌已保存至: {os.path.abspath(log_file)}")


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info(
        f"MES 詳細診斷工具啟動 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    diagnose_mes_queries()
    logger.info(
        f"MES 詳細診斷工具完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
