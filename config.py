#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class ETLConfig:
    """ETL系統配置類別"""
    
    # 分批處理設定
    BATCH_SIZE: int = 75
    PROGRESS_REPORT_INTERVAL: int = 10000
    
    # 錯誤處理設定
    MAX_RETRY_ATTEMPTS: int = 3
    RETRY_DELAY_SECONDS: int = 5
    
    # 資料庫超時設定
    CONNECTION_TIMEOUT: int = 30
    COMMAND_TIMEOUT: int = 300
    
    # 日誌設定
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # 安全設定
    ENCRYPT_CONNECTION: bool = True
    TRUST_SERVER_CERTIFICATE: bool = True
    
    # ETL摘要表設定
    ETL_SUMMARY_TABLE: str = "ETL_SUMMARY"
    
    # 檔案路徑設定
    DB_CONFIG_FILE: str = "db.json"
    QUERY_METADATA_FILE: str = "query_metadata.json"
    
    # 預設資料庫埠號
    DEFAULT_SQL_SERVER_PORT: int = 1433
    
    # SQL Server驅動程式
    SQL_SERVER_DRIVER: str = "ODBC Driver 17 for SQL Server"


class ConfigManager:
    """配置管理器，負責載入和管理所有配置"""
    
    def __init__(self, config_file: str = None, etl_config: ETLConfig = None):
        self.etl_config = etl_config or ETLConfig()
        self.db_config_file = config_file or self.etl_config.DB_CONFIG_FILE
        self._db_config = None
        self._query_metadata = None
        self.logger = self._setup_logger()
    
    def _setup_logger(self) -> logging.Logger:
        """設定日誌記錄器"""
        logger = logging.getLogger("ConfigManager")
        if not logger.handlers:  # 避免重複添加handler
            handler = logging.StreamHandler()
            formatter = logging.Formatter(self.etl_config.LOG_FORMAT)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(getattr(logging, self.etl_config.LOG_LEVEL))
        return logger
    
    def load_db_config(self) -> Dict[str, Any]:
        """載入資料庫配置"""
        if self._db_config is None:
            try:
                config_path = self._get_config_path(self.db_config_file)
                with open(config_path, 'r', encoding='utf-8') as f:
                    self._db_config = json.load(f)
                self.logger.info(f"成功載入資料庫配置: {config_path}")
            except FileNotFoundError:
                self.logger.error(f"資料庫配置檔案不存在: {self.db_config_file}")
                self._suggest_config_setup()
                sys.exit(1)
            except json.JSONDecodeError as e:
                self.logger.error(f"資料庫配置檔案格式錯誤: {e}")
                sys.exit(1)
            except Exception as e:
                self.logger.error(f"載入資料庫配置失敗: {e}")
                sys.exit(1)
        
        return self._db_config
    
    def load_query_metadata(self) -> Dict[str, Any]:
        """載入查詢中繼資料"""
        if self._query_metadata is None:
            try:
                metadata_path = self._get_config_path(self.etl_config.QUERY_METADATA_FILE)
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    self._query_metadata = json.load(f)
                self.logger.info(f"成功載入查詢中繼資料: {metadata_path}")
            except Exception as e:
                self.logger.error(f"載入查詢中繼資料失敗: {e}")
                sys.exit(1)
        
        return self._query_metadata
    
    def get_db_config(self, db_name: str) -> Dict[str, Any]:
        """取得特定資料庫的配置"""
        db_config = self.load_db_config()
        if db_name not in db_config:
            raise ValueError(f"找不到資料庫配置: {db_name}")
        return db_config[db_name]
    
    def get_queries_by_type(self, query_type: str) -> list:
        """根據類型過濾查詢"""
        metadata = self.load_query_metadata()
        return [q for q in metadata['queries'] if q['name'].startswith(f'{query_type}_')]
    
    def _get_config_path(self, filename: str) -> str:
        """取得配置檔案的完整路徑"""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(base_dir, filename)
    
    def _suggest_config_setup(self):
        """建議配置設定"""
        template_file = self._get_config_path("db.template.json")
        target_file = self._get_config_path(self.db_config_file)
        
        if os.path.exists(template_file):
            self.logger.info(f"請複製 {template_file} 到 {target_file} 並填入正確的資料庫資訊")
        else:
            self.logger.info(f"請創建 {target_file} 並參考專案文件填入資料庫配置")
    
    def validate_config(self) -> bool:
        """驗證配置的完整性"""
        try:
            # 驗證資料庫配置
            db_config = self.load_db_config()
            required_dbs = ['mes_db', 'sap_db', 'tableau_db']
            for db_name in required_dbs:
                if db_name not in db_config:
                    self.logger.error(f"缺少資料庫配置: {db_name}")
                    return False
                
                db_info = db_config[db_name]
                required_fields = ['server', 'database', 'username', 'password']
                for field in required_fields:
                    if field not in db_info or not db_info[field]:
                        self.logger.error(f"資料庫 {db_name} 缺少必要欄位: {field}")
                        return False
            
            # 驗證查詢中繼資料
            metadata = self.load_query_metadata()
            if 'queries' not in metadata:
                self.logger.error("查詢中繼資料缺少 'queries' 欄位")
                return False
            
            for query in metadata['queries']:
                required_fields = ['name', 'sql_file', 'target_table']
                for field in required_fields:
                    if field not in query:
                        self.logger.error(f"查詢 {query.get('name', 'unknown')} 缺少必要欄位: {field}")
                        return False
            
            self.logger.info("配置驗證通過")
            return True
            
        except Exception as e:
            self.logger.error(f"配置驗證失敗: {e}")
            return False


# 全域配置實例
config_manager = ConfigManager()

# 便利函數
def get_config_manager() -> ConfigManager:
    """取得全域配置管理器實例"""
    return config_manager

def get_etl_config() -> ETLConfig:
    """取得ETL配置"""
    return config_manager.etl_config

def get_db_config(db_name: str) -> Dict[str, Any]:
    """取得資料庫配置"""
    return config_manager.get_db_config(db_name)

def get_queries_by_type(query_type: str) -> list:
    """取得指定類型的查詢"""
    return config_manager.get_queries_by_type(query_type)