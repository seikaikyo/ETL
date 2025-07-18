#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import json
import datetime
import sys
import os
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text

# 日誌設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ETL_Process - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ETL_Process")

# 載入 DB 配置


def load_db_config(path="db.json"):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"載入 {path} 失敗: {e}")
        sys.exit(1)

# 建立 pyodbc 連線


def build_pyodbc_conn(cfg):
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
    return pyodbc.connect(conn_str)

# 建立 SQLAlchemy 引擎


def build_sqlalchemy_engine(cfg):
    drv = 'ODBC Driver 17 for SQL Server'.replace(' ', '+')
    srv = cfg['server']
    port = cfg.get('port', 1433)
    db = cfg['database']
    uid = cfg['username']
    pwd = cfg['password']
    opts = cfg.get('options', {})
    enc = 'yes' if opts.get('encrypt') else 'no'
    trust = 'yes' if opts.get('trustServerCertificate') else 'no'
    uri = (
        f"mssql+pyodbc://{uid}:{pwd}@{srv},{port}/{db}?driver={drv}"
        f"&Encrypt={enc}&TrustServerCertificate={trust}"
    )
    return create_engine(uri, fast_executemany=True)

# 讀取SQL文件


def load_sql_file(sql_file):
    try:
        # 取得當前腳本所在目錄的絕對路徑
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # 拼接SQL文件的絕對路徑
        sql_path = os.path.join(base_dir, sql_file)
        # 如果檔案不存在，嘗試在不同子目錄中查找
        if not os.path.exists(sql_path):
            # 根據查詢類型確定子目錄 (mes 或 sap)
            query_type = os.path.basename(sql_file).split('_')[0]
            sql_path_in_subdir = os.path.join(
                base_dir, query_type, os.path.basename(sql_file))
            # 嘗試在子目錄中查找SQL檔案
            if os.path.exists(sql_path_in_subdir):
                sql_path = sql_path_in_subdir
            else:
                # 如果仍找不到，記錄可能的路徑
                logger.debug(f"嘗試查找SQL檔案於: {sql_path} 和 {sql_path_in_subdir}")
                raise FileNotFoundError(f"找不到SQL檔案: {sql_file}")
        with open(sql_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"讀取SQL文件失敗: {sql_file}, {e}")
        sys.exit(1)

# 讀取查詢定義


def load_queries(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)['queries']
    except Exception as e:
        logger.error(f"載入查詢檔失敗: {path}, {e}")
        sys.exit(1)

# 確保 ETL_SUMMARY 表存在且結構正確


def ensure_etl_summary_table(engine):
    sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ETL_SUMMARY')
    BEGIN
        CREATE TABLE ETL_SUMMARY (
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
                       WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'SUMMARY_TYPE')
        BEGIN
            ALTER TABLE ETL_SUMMARY ADD [SUMMARY_TYPE] NVARCHAR(50) NULL
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                       WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'ETL_STATUS')
        BEGIN
            ALTER TABLE ETL_SUMMARY ADD [ETL_STATUS] NVARCHAR(50) NULL
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                       WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'mes_status')
        BEGIN
            ALTER TABLE ETL_SUMMARY ADD [mes_status] NVARCHAR(50) NULL
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                       WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'sap_status')
        BEGIN
            ALTER TABLE ETL_SUMMARY ADD [sap_status] NVARCHAR(50) NULL
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                       WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'mes_rows')
        BEGIN
            ALTER TABLE ETL_SUMMARY ADD [mes_rows] INT NULL
        END
        
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                       WHERE TABLE_NAME = 'ETL_SUMMARY' AND COLUMN_NAME = 'sap_rows')
        BEGIN
            ALTER TABLE ETL_SUMMARY ADD [sap_rows] INT NULL
        END
    END
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        logger.info("已確保 ETL_SUMMARY 表存在且結構正確")
    except Exception as e:
        logger.warning(f"檢查或創建 ETL_SUMMARY 表失敗: {e}")

# 檢查表是否存在


def check_table_exists(engine, table):
    sql = f"""
    SELECT CASE WHEN EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = '{table}'
    ) THEN 1 ELSE 0 END
    """
    with engine.connect() as conn:
        result = conn.execute(text(sql)).scalar()
    return result == 1

# 備份並清空目標表


def backup_and_truncate(engine, table):
    # 先檢查表是否存在
    if not check_table_exists(engine, table):
        logger.info(f"表 {table} 不存在，將創建新表")
        return None  # 不執行備份和清空，返回None表示沒有執行備份

    # 表存在，進行備份和清空
    ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    backup = f"{table}_backup_{ts}"
    try:
        with engine.begin() as conn:
            conn.execute(text(f"SELECT * INTO {backup} FROM {table}"))
            conn.execute(text(f"TRUNCATE TABLE {table}"))
        return backup
    except Exception as e:
        logger.warning(f"備份或清空表 {table} 失敗: {e}")
        return None

# 執行單筆 ETL - 使用分批處理避免參數過多錯誤，並處理NULL值


def run_etl(query, src_conn, tgt_engine):
    name = query['name']
    tgt = query['target_table']
    sql_file = query['sql_file']
    source_type = name.split('_')[0].upper()  # 從查詢名稱取得來源類型 (MES或SAP)

    # 從SQL文件讀取SQL語句
    sql = load_sql_file(sql_file)

    logger.info(f"處理查詢: {name}")
    try:
        df = pd.read_sql(sql, src_conn)

        # 處理NULL值
        if not df.empty:
            # 記錄NULL值情況
            null_counts = df.isnull().sum().sum()
            if null_counts > 0:
                logger.warning(f"查詢 {name} 包含 {null_counts} 個NULL值")

            # 針對數值型欄位，將NULL填充為0
            numeric_columns = df.select_dtypes(
                include=['int', 'float']).columns
            df[numeric_columns] = df[numeric_columns].fillna(0)

            # 針對字串型欄位，將NULL填充為空字串
            string_columns = df.select_dtypes(include=['object']).columns
            df[string_columns] = df[string_columns].fillna('')

        logger.info(f"讀取 {len(df)} 筆資料，處理後資料品質正常")
    except Exception as e:
        logger.error(f"執行查詢 {name} 失敗: {e}")
        raise

    if df.empty:
        logger.warning(f"查詢 {name} 未返回任何資料")
        # 記錄ETL執行結果
        try:
            with tgt_engine.begin() as conn:
                stmt = text(
                    "INSERT INTO ETL_SUMMARY ([TIMESTAMP], [SOURCE_TYPE], [QUERY_NAME], [TARGET_TABLE], [ROW_COUNT], [ETL_DATE], [SUMMARY_TYPE])"
                    " VALUES (GETDATE(), :source_type, :query_name, :target_table, 0, GETDATE(), 'QUERY')"
                )
                params = {
                    'source_type': source_type,
                    'query_name': name,
                    'target_table': tgt
                }
                conn.execute(stmt, params)
        except Exception as e:
            logger.warning(f"記錄空查詢執行結果失敗: {e}")

        return 0

    # 備份和清空表 (如果表存在)
    backup = backup_and_truncate(tgt_engine, tgt)
    if backup:
        logger.info(f"備份 {tgt} 至 {backup}，並清空目標表")

    # 分批處理，每批次最多75筆資料
    batch_size = 75
    total_rows = len(df)
    processed = 0

    try:
        # 使用if_exists='replace'來處理表不存在的情況
        for i in range(0, total_rows, batch_size):
            chunk = df.iloc[i:min(i+batch_size, total_rows)]
            # 首次迭代使用replace，後續使用append
            mode = 'replace' if i == 0 else 'append'
            chunk.to_sql(tgt, tgt_engine, if_exists=mode,
                         index=False, method=None)
            processed += len(chunk)
            if processed % 500 == 0 or processed == total_rows:
                logger.info(
                    f"進度: {processed}/{total_rows} 筆 ({int(processed/total_rows*100)}%)")

        logger.info(f"已匯入總計 {total_rows} 筆至 {tgt}")

        # 記錄ETL執行結果
        try:
            with tgt_engine.begin() as conn:
                stmt = text(
                    "INSERT INTO ETL_SUMMARY ([TIMESTAMP], [SOURCE_TYPE], [QUERY_NAME], [TARGET_TABLE], [ROW_COUNT], [ETL_DATE], [SUMMARY_TYPE])"
                    " VALUES (GETDATE(), :source_type, :query_name, :target_table, :row_count, GETDATE(), 'QUERY')"
                )
                params = {
                    'source_type': source_type,
                    'query_name': name,
                    'target_table': tgt,
                    'row_count': total_rows
                }
                conn.execute(stmt, params)
        except Exception as e:
            logger.warning(f"記錄ETL執行結果失敗: {e}")

        return total_rows
    except Exception as e:
        logger.error(f"匯入資料至 {tgt} 失敗: {e}")

        # 還原備份 (如果有)
        if backup:
            try:
                with tgt_engine.begin() as conn:
                    if check_table_exists(tgt_engine, tgt):
                        conn.execute(text(f"DROP TABLE {tgt}"))
                    conn.execute(text(f"SELECT * INTO {tgt} FROM {backup}"))
                logger.info(f"已還原 {tgt} 從備份 {backup}")
            except Exception as restore_err:
                logger.error(f"還原備份失敗: {restore_err}")

        raise

# 增加一個用於記錄整體ETL執行摘要的函數，修改以解決空行問題


def record_etl_summary(engine, mes_status, sap_status, mes_rows, sap_rows):
    """記錄整體ETL執行摘要"""
    try:
        # 使用更清晰的標記和完整資訊
        with engine.begin() as conn:
            stmt = text(
                "INSERT INTO ETL_SUMMARY ([TIMESTAMP], [SOURCE_TYPE], [QUERY_NAME], [TARGET_TABLE], [ROW_COUNT], [ETL_DATE], "
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
        logger.info("已記錄ETL執行摘要")
    except Exception as e:
        logger.warning(f"記錄ETL執行摘要失敗: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true', help='執行所有ETL')
    parser.add_argument('--mes', action='store_true', help='只執行MES ETL')
    parser.add_argument('--sap', action='store_true', help='只執行SAP ETL')
    parser.add_argument('--debug', action='store_true', help='啟用詳細的除錯訊息')
    args = parser.parse_args()

    # 設定日誌等級
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("已啟用詳細除錯模式")

    logger.info('='*60)
    logger.info(f"ETL 程序啟動 - {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")

    # 載入 DB 設定
    cfg = load_db_config('db.json')
    try:
        src_mes = build_pyodbc_conn(cfg['mes_db'])
        logger.info("成功連接到MES資料庫")
    except Exception as e:
        logger.error(f"連接MES資料庫失敗: {e}")
        src_mes = None

    try:
        src_sap = build_pyodbc_conn(cfg['sap_db'])
        logger.info("成功連接到SAP資料庫")
    except Exception as e:
        logger.error(f"連接SAP資料庫失敗: {e}")
        src_sap = None

    try:
        tgt_engine = build_sqlalchemy_engine(cfg['tableau_db'])
        logger.info("成功連接到Tableau資料庫")
    except Exception as e:
        logger.error(f"連接Tableau資料庫失敗: {e}")
        sys.exit(1)  # 如果無法連接到目標資料庫，則終止程序

    # 確保 ETL_SUMMARY 表結構正確
    ensure_etl_summary_table(tgt_engine)

    # 載入查詢定義
    try:
        query_metadata = load_queries('query_metadata.json')
        mes_q = [q for q in query_metadata if q['name'].startswith('mes_')]
        sap_q = [q for q in query_metadata if q['name'].startswith('sap_')]
        logger.info(f"已載入查詢定義: MES={len(mes_q)}個, SAP={len(sap_q)}個")
    except Exception as e:
        logger.error(f"載入查詢定義失敗: {e}")
        sys.exit(1)

    mes_stat = sap_stat = '跳過'
    mes_cnt = sap_cnt = 0

    # 執行 MES ETL
    if (args.all or args.mes) and src_mes is not None:
        logger.info('開始 MES ETL 流程...')
        try:
            for q in mes_q:
                logger.debug(f"執行MES查詢: {q['name']}")
                rows = run_etl(q, src_mes, tgt_engine)
                mes_cnt += rows
            mes_stat = '成功'
            logger.info(f"MES ETL 完成，處理 {mes_cnt} 筆資料")
        except Exception as e:
            logger.error(f"MES ETL 失敗: {e}")
            mes_stat = '失敗'
    elif src_mes is None and (args.all or args.mes):
        logger.error("無法執行MES ETL: MES資料庫連線失敗")
        mes_stat = '失敗'

    # 執行 SAP ETL
    if (args.all or args.sap) and src_sap is not None:
        logger.info('開始 SAP ETL 流程...')
        try:
            for q in sap_q:
                logger.debug(f"執行SAP查詢: {q['name']}")
                rows = run_etl(q, src_sap, tgt_engine)
                sap_cnt += rows
            sap_stat = '成功'
            logger.info(f"SAP ETL 完成，處理 {sap_cnt} 筆資料")
        except Exception as e:
            logger.error(f"SAP ETL 失敗: {e}")
            sap_stat = '失敗'
    elif src_sap is None and (args.all or args.sap):
        logger.error("無法執行SAP ETL: SAP資料庫連線失敗")
        sap_stat = '失敗'

    logger.info('='*60)
    logger.info(
        f"ETL 執行結果 - MES: {mes_stat} ({mes_cnt}筆), SAP: {sap_stat} ({sap_cnt}筆)")

    # 記錄整體ETL執行摘要
    record_etl_summary(tgt_engine, mes_stat, sap_stat, mes_cnt, sap_cnt)

    # 關閉資料庫連線
    if src_mes is not None:
        src_mes.close()
    if src_sap is not None:
        src_sap.close()

    if mes_stat == '失敗' or sap_stat == '失敗':
        logger.error('ETL 處理有部分失敗！')
        sys.exit(1)
    sys.exit(0)
