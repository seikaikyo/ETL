#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import json
import datetime
import os
import pandas as pd
import pyodbc
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - ETL_Process - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ETL_Process")


def load_queries(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)['queries']


def get_conn(conn_str):
    return pyodbc.connect(conn_str)


def backup_and_truncate(engine, table):
    ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    backup = f"{table}_backup_{ts}"
    with engine.begin() as conn:
        conn.execute(f"SELECT * INTO {backup} FROM {table}")
        conn.execute(f"TRUNCATE TABLE {table}")
    return backup


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

    mes_stat = '跳過'
    sap_stat = '跳過'
    mes_cnt = 0
    sap_cnt = 0
    mes_q = load_queries('mes_queries.json')
    sap_q = load_queries('sap_queries.json')
    src_mes = get_conn(os.getenv('MES_CONN'))
    src_sap = get_conn(os.getenv('SAP_CONN'))
    tgt_eng = create_engine(os.getenv('TARGET_CONN'))

    if args.all or args.mes:
        logger.info('開始 MES ETL 流程...')
        try:
            for q in mes_q:
                mes_cnt = run_etl(q, src_mes, tgt_eng)
            mes_stat = '成功'
        except Exception as e:
            logger.error(f"MES ETL 失敗: {e}")
            mes_stat = '失敗'

    if args.all or args.sap:
        logger.info('開始 SAP ETL 流程...')
        try:
            for q in sap_q:
                sap_cnt = run_etl(q, src_sap, tgt_eng)
            sap_stat = '成功'
        except Exception as e:
            logger.error(f"SAP ETL 失敗: {e}")
            sap_stat = '失敗'

    record_summary(tgt_eng, mes_stat, sap_stat, mes_cnt, sap_cnt)
    logger.info('='*60)
    logger.info(f"ETL 執行結果 - MES: {mes_stat}, SAP: {sap_stat}")
    if mes_stat == '失敗' or sap_stat == '失敗':
        logger.error('ETL 處理有部分失敗！')
        exit(1)
    exit(0)
