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
        sql_path = os.path.join(base_dir, 'queries', sql_file)

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

# 備份並清空目標表


def backup_and_truncate(engine, table):
    ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    backup = f"{table}_backup_{ts}"
    with engine.begin() as conn:
        conn.execute(text(f"SELECT * INTO {backup} FROM {table}"))
        conn.execute(text(f"TRUNCATE TABLE {table}"))
    return backup

# 執行單筆 ETL


def run_etl(query, src_conn, tgt_engine):
    name = query['name']
    tgt = query['target_table']
    sql_file = query['sql_file']

    # 從SQL文件讀取SQL語句
    sql = load_sql_file(sql_file)

    logger.info(f"處理查詢: {name}")
    df = pd.read_sql(sql, src_conn)
    logger.info(f"讀取 {len(df)} 筆資料")
    backup = backup_and_truncate(tgt_engine, tgt)
    logger.info(f"備份 {tgt} 至 {backup}，並清空目標表")
    df.to_sql(tgt, tgt_engine, if_exists='append',
              index=False, chunksize=1000, method='multi')
    logger.info(f"已匯入 {len(df)} 筆至 {tgt}")
    return len(df)

# 記錄 ETL 摘要，若失敗則警告


def record_summary(engine, mes_stat, sap_stat, mes_cnt, sap_cnt):
    now = datetime.datetime.now()
    try:
        stmt = text(
            "INSERT INTO ETL_SUMMARY (run_time, mes_status, sap_status, mes_rows, sap_rows)"
            " VALUES (:run_time, :mes_status, :sap_status, :mes_rows, :sap_rows)"
        )
        params = {
            'run_time': now,
            'mes_status': mes_stat,
            'sap_status': sap_stat,
            'mes_rows': mes_cnt,
            'sap_rows': sap_cnt
        }
        with engine.begin() as conn:
            conn.execute(stmt, params)
        logger.info("ETL 摘要記錄已保存")
    except Exception as e:
        logger.warning(f"記錄 ETL_SUMMARY 失敗: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--mes', action='store_true')
    parser.add_argument('--sap', action='store_true')
    args = parser.parse_args()

    logger.info('='*60)
    logger.info(f"ETL 程序啟動 - {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")

    # 載入 DB 設定
    cfg = load_db_config('db.json')
    src_mes = build_pyodbc_conn(cfg['mes_db'])
    src_sap = build_pyodbc_conn(cfg['sap_db'])
    tgt_engine = build_sqlalchemy_engine(cfg['tableau_db'])

    # 載入查詢定義
    query_metadata = load_queries('query_metadata.json')
    mes_q = [q for q in query_metadata if q['name'].startswith('mes_')]
    sap_q = [q for q in query_metadata if q['name'].startswith('sap_')]

    mes_stat = sap_stat = '跳過'
    mes_cnt = sap_cnt = 0

    # 執行 MES ETL
    if args.all or args.mes:
        logger.info('開始 MES ETL 流程...')
        try:
            for q in mes_q:
                mes_cnt = run_etl(q, src_mes, tgt_engine)
            mes_stat = '成功'
        except Exception as e:
            logger.error(f"MES ETL 失敗: {e}")
            mes_stat = '失敗'

    # 執行 SAP ETL
    if args.all or args.sap:
        logger.info('開始 SAP ETL 流程...')
        try:
            for q in sap_q:
                sap_cnt = run_etl(q, src_sap, tgt_engine)
            sap_stat = '成功'
        except Exception as e:
            logger.error(f"SAP ETL 失敗: {e}")
            sap_stat = '失敗'

    # 記錄摘要
    record_summary(tgt_engine, mes_stat, sap_stat, mes_cnt, sap_cnt)

    logger.info('='*60)
    logger.info(f"ETL 執行結果 - MES: {mes_stat}, SAP: {sap_stat}")
    if mes_stat == '失敗' or sap_stat == '失敗':
        logger.error('ETL 處理有部分失敗！')
        sys.exit(1)
    sys.exit(0)
