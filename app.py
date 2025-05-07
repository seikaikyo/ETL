#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import json
import datetime
import os
import sys
import pandas as pd
import pyodbc
from sqlalchemy import create_engine

# 設定日誌格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ETL_Process - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ETL_Process")

# 讀取查詢定義檔


def load_queries(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)['queries']
    except Exception as e:
        logger.error(f"載入查詢檔失敗: {path}, {e}")
        sys.exit(1)

# 取得資料庫連線


def get_pyodbc_conn(conn_str, name):
    if not conn_str:
        logger.error(f"環境變數 {name} 未設定")
        sys.exit(1)
    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        logger.error(f"無法連線至 {name}: {e}")
        sys.exit(1)

# 建立 SQLAlchemy 引擎


def get_engine(uri):
    if not uri:
        logger.error("環境變數 TARGET_CONN 未設定")
        sys.exit(1)
    try:
        return create_engine(uri)
    except Exception as e:
        logger.error(f"無法建立目標資料庫引擎: {e}")
        sys.exit(1)

# 備份並清空目標表


def backup_and_truncate(engine, table):
    ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    backup = f"{table}_backup_{ts}"
    with engine.begin() as conn:
        conn.execute(f"SELECT * INTO {backup} FROM {table}")
        conn.execute(f"TRUNCATE TABLE {table}")
    return backup

# 執行單一 ETL 查詢


def run_etl(query, src_conn, tgt_engine):
    name = query['name']
    tgt = query['target_table']
    sql = query['sql']
    logger.info(f"處理查詢: {name}")
    df = pd.read_sql(sql, src_conn)
    logger.info(f"讀取 {len(df)} 筆資料")
    backup = backup_and_truncate(tgt_engine, tgt)
    logger.info(f"備份至 {backup} 並清空 {tgt}")
    df.to_sql(tgt, tgt_engine, if_exists='append',
              index=False, chunksize=1000, method='multi')
    logger.info(f"已匯入 {len(df)} 筆至 {tgt}")
    return len(df)

# 記錄執行摘要


def record_summary(engine, mes_stat, sap_stat, mes_cnt, sap_cnt):
    now = datetime.datetime.now()
    rec = pd.DataFrame([{
        'run_time': now,
        'mes_status': mes_stat,
        'sap_status': sap_stat,
        'mes_rows': mes_cnt,
        'sap_rows': sap_cnt
    }])
    rec.to_sql('ETL_SUMMARY', engine, if_exists='append', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true')
    parser.add_argument('--mes', action='store_true')
    parser.add_argument('--sap', action='store_true')
    args = parser.parse_args()

    logger.info('='*60)
    logger.info(f"ETL 程序啟動 - {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")

    # 環境變數讀取
    mes_conn_str = os.getenv('MES_CONN')
    sap_conn_str = os.getenv('SAP_CONN')
    target_conn_uri = os.getenv('TARGET_CONN')

    # 建立連線
    src_mes = get_pyodbc_conn(mes_conn_str, 'MES_CONN')
    src_sap = get_pyodbc_conn(sap_conn_str, 'SAP_CONN')
    tgt_eng = get_engine(target_conn_uri)

    # 載入查詢
    mes_q = load_queries('mes_queries.json')
    sap_q = load_queries('sap_queries.json')

    mes_stat, sap_stat = '跳過', '跳過'
    mes_cnt = sap_cnt = 0

    # MES ETL
    if args.all or args.mes:
        logger.info('開始 MES ETL 流程...')
        try:
            for q in mes_q:
                mes_cnt = run_etl(q, src_mes, tgt_eng)
            mes_stat = '成功'
        except Exception as e:
            logger.error(f"MES ETL 失敗: {e}")
            mes_stat = '失敗'

    # SAP ETL
    if args.all or args.sap:
        logger.info('開始 SAP ETL 流程...')
        try:
            for q in sap_q:
                sap_cnt = run_etl(q, src_sap, tgt_eng)
            sap_stat = '成功'
        except Exception as e:
            logger.error(f"SAP ETL 失敗: {e}")
            sap_stat = '失敗'

    # 記錄摘要
    record_summary(tgt_eng, mes_stat, sap_stat, mes_cnt, sap_cnt)
    logger.info('='*60)
    logger.info(f"ETL 執行結果 - MES: {mes_stat}, SAP: {sap_stat}")
    if mes_stat == '失敗' or sap_stat == '失敗':
        logger.error('ETL 處理有部分失敗！')
        sys.exit(1)
    sys.exit(0)
