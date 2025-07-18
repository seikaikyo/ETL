#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import sys
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

# 導入自定義模組
from config import get_config_manager, get_etl_config
from database import DatabaseManager
from sql_loader import SQLLoader


class ETLDiagnostics:
    """ETL診斷工具 - 使用重構後的安全架構"""
    
    def __init__(self, config_manager, db_manager: DatabaseManager, sql_loader: SQLLoader):
        self.config_manager = config_manager
        self.db_manager = db_manager
        self.sql_loader = sql_loader
        self.etl_config = config_manager.etl_config
        self.logger = logging.getLogger("ETL_Diagnostics")
    
    def check_database_connections(self) -> Dict[str, bool]:
        """檢查所有資料庫連線"""
        db_configs = ['mes_db', 'sap_db', 'tableau_db']
        results = {}
        
        self.logger.info("檢查資料庫連線...")
        for db_name in db_configs:
            try:
                result = self.db_manager.test_connection(db_name)
                results[db_name] = result
                status = "成功" if result else "失敗"
                self.logger.info(f"{db_name}: {status}")
            except Exception as e:
                results[db_name] = False
                self.logger.error(f"{db_name}: 失敗 - {e}")
        
        return results
    
    def check_table_existence(self, db_name: str, expected_tables: List[str]) -> Dict[str, bool]:
        """檢查關鍵資料表是否存在"""
        results = {}
        self.logger.info(f"檢查 {db_name} 的關鍵資料表...")
        
        for table in expected_tables:
            try:
                exists = self.db_manager.check_table_exists(db_name, table)
                results[table] = exists
                status = "存在" if exists else "不存在"
                self.logger.info(f"  {table}: {status}")
            except Exception as e:
                results[table] = False
                self.logger.error(f"  {table}: 檢查失敗 - {e}")
        
        return results
    
    def check_query_syntax(self, queries: List[Dict[str, Any]]) -> Dict[str, bool]:
        """檢查SQL查詢語法"""
        results = {}
        self.logger.info("檢查SQL查詢語法...")
        
        for query in queries:
            query_name = query['name']
            sql_file = query['sql_file']
            
            try:
                # 載入SQL文件
                sql_content = self.sql_loader.load_sql_file(sql_file)
                
                # 基本語法檢查（檢查是否為空、是否包含基本SQL關鍵字）
                if not sql_content.strip():
                    results[query_name] = False
                    self.logger.error(f"  {query_name}: SQL內容為空")
                    continue
                
                sql_lower = sql_content.lower()
                if 'select' not in sql_lower:
                    results[query_name] = False
                    self.logger.error(f"  {query_name}: 不是有效的SELECT查詢")
                    continue
                
                results[query_name] = True
                self.logger.info(f"  {query_name}: 語法檢查通過")
                
            except Exception as e:
                results[query_name] = False
                self.logger.error(f"  {query_name}: 語法檢查失敗 - {e}")
        
        return results
    
    def test_query_execution(self, source_db: str, queries: List[Dict[str, Any]], limit: int = 5) -> Dict[str, Dict[str, Any]]:
        """測試查詢執行（限制返回行數）"""
        results = {}
        self.logger.info(f"測試 {source_db} 查詢執行（限制 {limit} 行）...")
        
        for query in queries:
            query_name = query['name']
            sql_file = query['sql_file']
            
            try:
                # 載入SQL並修改為限制行數
                sql_content = self.sql_loader.load_sql_file(sql_file)
                
                # 在SQL前面加上TOP限制（僅適用於SQL Server）
                if sql_content.strip().lower().startswith('select'):
                    # 如果已經有TOP，則不添加
                    if 'top ' not in sql_content.lower()[:100]:
                        sql_content = sql_content.replace('SELECT', f'SELECT TOP {limit}', 1)
                
                # 執行查詢
                with self.db_manager.get_connection_context(source_db) as conn:
                    df = pd.read_sql(sql_content, conn)
                
                results[query_name] = {
                    'success': True,
                    'row_count': len(df),
                    'column_count': len(df.columns) if not df.empty else 0,
                    'columns': list(df.columns) if not df.empty else [],
                    'null_counts': df.isnull().sum().sum() if not df.empty else 0,
                    'error': None
                }
                
                self.logger.info(f"  {query_name}: 成功 - {len(df)} 行, {len(df.columns)} 列")
                
                if not df.empty and df.isnull().sum().sum() > 0:
                    self.logger.warning(f"  {query_name}: 包含 {df.isnull().sum().sum()} 個NULL值")
                
            except Exception as e:
                results[query_name] = {
                    'success': False,
                    'row_count': 0,
                    'column_count': 0,
                    'columns': [],
                    'null_counts': 0,
                    'error': str(e)
                }
                self.logger.error(f"  {query_name}: 執行失敗 - {e}")
        
        return results
    
    def check_target_table_compatibility(self, target_db: str, queries: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """檢查目標表的相容性"""
        results = {}
        self.logger.info("檢查目標表相容性...")
        
        for query in queries:
            query_name = query['name']
            target_table = query['target_table']
            
            try:
                # 檢查目標表是否存在
                exists = self.db_manager.check_table_exists(target_db, target_table)
                
                if exists:
                    # 取得表結構
                    structure = self.db_manager.get_table_structure(target_db, target_table)
                    results[query_name] = {
                        'table_exists': True,
                        'column_count': len(structure),
                        'columns': [col['name'] for col in structure],
                        'structure': structure
                    }
                    self.logger.info(f"  {target_table}: 存在 - {len(structure)} 個欄位")
                else:
                    results[query_name] = {
                        'table_exists': False,
                        'column_count': 0,
                        'columns': [],
                        'structure': []
                    }
                    self.logger.info(f"  {target_table}: 不存在（將自動創建）")
                
            except Exception as e:
                results[query_name] = {
                    'table_exists': False,
                    'column_count': 0,
                    'columns': [],
                    'structure': [],
                    'error': str(e)
                }
                self.logger.error(f"  {target_table}: 檢查失敗 - {e}")
        
        return results
    
    def generate_diagnostic_report(self, results: Dict[str, Any]) -> str:
        """生成診斷報告"""
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append(f"ETL 系統診斷報告 - {datetime.now():%Y-%m-%d %H:%M:%S}")
        report_lines.append("=" * 80)
        
        # 資料庫連線狀態
        if 'connections' in results:
            report_lines.append("\n## 資料庫連線狀態")
            for db_name, status in results['connections'].items():
                status_text = "✓ 成功" if status else "✗ 失敗"
                report_lines.append(f"  {db_name}: {status_text}")
        
        # 關鍵表檢查
        if 'tables' in results:
            report_lines.append("\n## 關鍵資料表檢查")
            for db_name, tables in results['tables'].items():
                report_lines.append(f"\n### {db_name}")
                for table_name, exists in tables.items():
                    status_text = "✓ 存在" if exists else "✗ 不存在"
                    report_lines.append(f"  {table_name}: {status_text}")
        
        # SQL語法檢查
        if 'sql_syntax' in results:
            report_lines.append("\n## SQL查詢語法檢查")
            for query_name, valid in results['sql_syntax'].items():
                status_text = "✓ 通過" if valid else "✗ 失敗"
                report_lines.append(f"  {query_name}: {status_text}")
        
        # 查詢執行測試
        if 'query_execution' in results:
            report_lines.append("\n## 查詢執行測試")
            for db_name, queries in results['query_execution'].items():
                report_lines.append(f"\n### {db_name}")
                for query_name, result in queries.items():
                    if result['success']:
                        report_lines.append(f"  ✓ {query_name}: {result['row_count']} 行, {result['column_count']} 列")
                        if result['null_counts'] > 0:
                            report_lines.append(f"    ⚠ 包含 {result['null_counts']} 個NULL值")
                    else:
                        report_lines.append(f"  ✗ {query_name}: {result['error']}")
        
        # 目標表相容性
        if 'target_tables' in results:
            report_lines.append("\n## 目標表相容性檢查")
            for query_name, result in results['target_tables'].items():
                if result['table_exists']:
                    report_lines.append(f"  ✓ {query_name}: 表存在 ({result['column_count']} 欄位)")
                else:
                    report_lines.append(f"  ⚠ {query_name}: 表不存在，將自動創建")
        
        report_lines.append("\n" + "=" * 80)
        
        return "\n".join(report_lines)
    
    def run_full_diagnostics(self) -> Dict[str, Any]:
        """執行完整診斷"""
        results = {}
        
        # 1. 檢查資料庫連線
        results['connections'] = self.check_database_connections()
        
        # 2. 檢查關鍵資料表
        results['tables'] = {}
        
        # MES關鍵表
        if results['connections'].get('mes_db', False):
            mes_tables = ['MANUFACTURING_NO', 'FEED_MATERIAL_DEVICE', 'BOM_COMPONENT', 'OPERATION']
            results['tables']['mes_db'] = self.check_table_existence('mes_db', mes_tables)
        
        # SAP關鍵表（根據實際情況調整）
        if results['connections'].get('sap_db', False):
            sap_tables = ['PRODUCTION_ORDER']  # 根據實際SAP表名調整
            results['tables']['sap_db'] = self.check_table_existence('sap_db', sap_tables)
        
        # Tableau表
        if results['connections'].get('tableau_db', False):
            tableau_tables = [self.etl_config.ETL_SUMMARY_TABLE]
            results['tables']['tableau_db'] = self.check_table_existence('tableau_db', tableau_tables)
        
        # 3. 檢查SQL語法
        all_queries = self.config_manager.load_query_metadata()['queries']
        results['sql_syntax'] = self.check_query_syntax(all_queries)
        
        # 4. 測試查詢執行
        results['query_execution'] = {}
        
        if results['connections'].get('mes_db', False):
            mes_queries = self.config_manager.get_queries_by_type('mes')
            if mes_queries:
                results['query_execution']['mes_db'] = self.test_query_execution('mes_db', mes_queries, limit=3)
        
        if results['connections'].get('sap_db', False):
            sap_queries = self.config_manager.get_queries_by_type('sap')
            if sap_queries:
                results['query_execution']['sap_db'] = self.test_query_execution('sap_db', sap_queries, limit=3)
        
        # 5. 檢查目標表相容性
        if results['connections'].get('tableau_db', False):
            results['target_tables'] = self.check_target_table_compatibility('tableau_db', all_queries)
        
        return results


def setup_logging(debug: bool = False) -> logging.Logger:
    """設定日誌記錄器"""
    level = logging.DEBUG if debug else logging.INFO
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    return logging.getLogger("ETL_Diagnostics")


def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(description='ETL 系統診斷工具')
    parser.add_argument('--debug', action='store_true', help='啟用詳細的除錯訊息')
    parser.add_argument('--config', help='指定配置檔案路徑', default='db.json')
    parser.add_argument('--output', help='指定報告輸出檔案路徑')
    parser.add_argument('--connections-only', action='store_true', help='僅檢查資料庫連線')
    args = parser.parse_args()
    
    # 設定日誌
    logger = setup_logging(args.debug)
    
    try:
        # 初始化配置管理器
        config_manager = get_config_manager()
        if args.config != 'db.json':
            config_manager.db_config_file = args.config
        
        # 驗證配置
        if not config_manager.validate_config():
            logger.error("配置驗證失敗，終止程式")
            sys.exit(1)
        
        # 初始化組件
        db_manager = DatabaseManager(config_manager)
        sql_loader = SQLLoader(config_manager)
        diagnostics = ETLDiagnostics(config_manager, db_manager, sql_loader)
        
        logger.info("開始ETL系統診斷...")
        
        if args.connections_only:
            # 僅檢查連線
            results = {'connections': diagnostics.check_database_connections()}
        else:
            # 完整診斷
            results = diagnostics.run_full_diagnostics()
        
        # 生成報告
        report = diagnostics.generate_diagnostic_report(results)
        
        # 輸出報告
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(report)
            logger.info(f"診斷報告已保存至: {args.output}")
        else:
            print(report)
        
        # 檢查是否有嚴重問題
        connection_issues = sum(1 for status in results.get('connections', {}).values() if not status)
        if connection_issues > 0:
            logger.warning(f"發現 {connection_issues} 個資料庫連線問題")
            sys.exit(1)
        
        logger.info("診斷完成")
        
    except Exception as e:
        logger.error(f"診斷程序執行失敗: {e}")
        sys.exit(1)
    
    finally:
        # 清理資源
        try:
            if 'db_manager' in locals():
                db_manager.close_connections()
            if 'sql_loader' in locals():
                sql_loader.clear_cache()
        except:
            pass


if __name__ == "__main__":
    main()