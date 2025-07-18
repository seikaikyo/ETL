#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pyodbc
import pandas as pd
import argparse
import sys

# 載入資料庫配置


def load_db_config(path="db.json"):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"載入 {path} 失敗: {e}")
        sys.exit(1)

# 建立資料庫連線


def build_connection(cfg):
    drv = "{ODBC Driver 17 for SQL Server}"
    srv = cfg['server']
    port = cfg.get('port', 1433)
    db = cfg['database']
    uid = cfg['username']
    pwd = cfg['password']
    opts = cfg.get('options', {})
    enc = 'yes' if opts.get('encrypt') else 'no'
    trust = 'yes' if opts.get('trustServerCertificate') else 'no'

    conn_str = (
        f"DRIVER={drv};"
        f"SERVER={srv},{port};DATABASE={db};"
        f"UID={uid};PWD={pwd};Encrypt={enc};TrustServerCertificate={trust};"
    )

    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        print(f"連接資料庫失敗: {e}")
        sys.exit(1)

# 檢查資料表情況


def check_tables(conn):
    cursor = conn.cursor()
    print("=== 檢查資料表 ===")

    # 取得所有資料表
    cursor.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME")
    tables = [row.TABLE_NAME for row in cursor.fetchall()]

    print(f"找到 {len(tables)} 個資料表")
    for table in tables[:10]:  # 只顯示前10個
        print(f"- {table}")
    if len(tables) > 10:
        print(f"... 另外還有 {len(tables)-10} 個資料表")

    # 檢查關鍵資料表
    key_tables = ['MANUFACTURING_NO',
                  'FEED_MATERIAL_DEVICE', 'BOM_COMPONENT', 'OPERATION']
    for table in key_tables:
        if table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"表 {table} 含有 {count} 筆資料")

            # 檢查NULL值
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE 1=2")
            columns = [column[0] for column in cursor.description]

            for col in columns:
                cursor.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
                null_count = cursor.fetchone()[0]
                if null_count > 0:
                    percent = (null_count / count) * 100 if count > 0 else 0
                    print(
                        f"  - 欄位 {col} 有 {null_count} 筆NULL值 ({percent:.1f}%)")
        else:
            print(f"警告: 找不到關鍵資料表 {table}")

# 執行診斷查詢


def run_diagnostics(conn, sql_file=None):
    print("\n=== 執行診斷查詢 ===")

    if sql_file:
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql = f.read()
            print(f"使用SQL檔案: {sql_file}")
        except Exception as e:
            print(f"讀取SQL檔案失敗: {e}")
            return
    else:
        # 預設診斷查詢
        sql = """
        WITH SampleOrders AS (
            SELECT TOP 10 MANUFACTURING_OD
            FROM MANUFACTURING_NO
            ORDER BY MANUFACTURING_OD DESC
        )
        SELECT 
            mn.MANUFACTURING_OD,
            mn.MATERIAL,
            mn.QTY,
            fm.MATERIAL AS FM_MATERIAL,
            fm.OPERATION,
            fm.IN_QTY,
            bc.COMPONENT,
            bc.QTY AS BOM_QTY
        FROM SampleOrders so
        JOIN MANUFACTURING_NO mn ON so.MANUFACTURING_OD = mn.MANUFACTURING_OD
        LEFT JOIN FEED_MATERIAL_DEVICE fm ON mn.MANUFACTURING_OD = fm.MANUFACTURING_OD
        LEFT JOIN BOM_COMPONENT bc ON mn.MANUFACTURING_OD = bc.BOM AND fm.MATERIAL = bc.COMPONENT
        """

    try:
        df = pd.read_sql(sql, conn)
        if df.empty:
            print("查詢未返回任何資料")
        else:
            print(f"查詢返回 {len(df)} 筆資料")
            print("\n資料範例:")
            print(df.head().to_string())

            # 檢查NULL值
            null_counts = df.isnull().sum()
            print("\nNULL值統計:")
            for col, count in null_counts.items():
                if count > 0:
                    percent = (count / len(df)) * 100
                    print(f"  - {col}: {count} ({percent:.1f}%)")
    except Exception as e:
        print(f"執行診斷查詢失敗: {e}")


def main():
    parser = argparse.ArgumentParser(description="MES資料診斷工具")
    parser.add_argument("--sql", help="要執行的SQL檔案路徑")
    args = parser.parse_args()

    print("=== MES資料診斷工具 ===")

    # 載入資料庫配置
    cfg = load_db_config()
    mes_cfg = cfg.get('mes_db')
    if not mes_cfg:
        print("錯誤: 找不到MES資料庫配置")
        sys.exit(1)

    # 連接MES資料庫
    print(f"連接到MES資料庫: {mes_cfg['server']}/{mes_cfg['database']}")
    conn = build_connection(mes_cfg)

    # 執行診斷
    check_tables(conn)
    run_diagnostics(conn, args.sql)

    # 關閉連線
    conn.close()
    print("\n診斷完成")


if __name__ == "__main__":
    main()
