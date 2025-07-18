#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pyodbc
import logging
import time
import sys
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from typing import Dict, Any, Optional, Union
from urllib.parse import quote_plus

from config import get_etl_config, ConfigManager


class DatabaseManager:
    """資料庫連線管理器 - 提供安全的資料庫操作"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.etl_config = config_manager.etl_config
        self.logger = logging.getLogger("DatabaseManager")
        self._connections = {}
        self._engines = {}
    
    def build_connection_string(self, db_config: Dict[str, Any]) -> str:
        """建立安全的資料庫連接字串"""
        server = db_config['server']
        port = db_config.get('port', self.etl_config.DEFAULT_SQL_SERVER_PORT)
        database = db_config['database']
        username = db_config['username']
        password = db_config['password']
        options = db_config.get('options', {})
        
        # 安全設定
        encrypt = 'yes' if options.get('encrypt', self.etl_config.ENCRYPT_CONNECTION) else 'no'
        trust_cert = 'yes' if options.get('trustServerCertificate', self.etl_config.TRUST_SERVER_CERTIFICATE) else 'no'
        
        connection_string = (
            f"DRIVER={{{self.etl_config.SQL_SERVER_DRIVER}}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"Encrypt={encrypt};"
            f"TrustServerCertificate={trust_cert};"
            f"Connection Timeout={self.etl_config.CONNECTION_TIMEOUT};"
        )
        
        return connection_string
    
    def build_sqlalchemy_uri(self, db_config: Dict[str, Any]) -> str:
        """建立SQLAlchemy連接URI"""
        server = db_config['server']
        port = db_config.get('port', self.etl_config.DEFAULT_SQL_SERVER_PORT)
        database = db_config['database']
        username = quote_plus(db_config['username'])
        password = quote_plus(db_config['password'])
        options = db_config.get('options', {})
        
        driver = quote_plus(self.etl_config.SQL_SERVER_DRIVER)
        encrypt = 'yes' if options.get('encrypt', self.etl_config.ENCRYPT_CONNECTION) else 'no'
        trust_cert = 'yes' if options.get('trustServerCertificate', self.etl_config.TRUST_SERVER_CERTIFICATE) else 'no'
        
        uri = (
            f"mssql+pyodbc://{username}:{password}@{server},{port}/{database}"
            f"?driver={driver}&Encrypt={encrypt}&TrustServerCertificate={trust_cert}"
        )
        
        return uri
    
    def get_connection(self, db_name: str, force_new: bool = False) -> pyodbc.Connection:
        """取得資料庫連線，支援連線重用"""
        if force_new or db_name not in self._connections:
            try:
                db_config = self.config_manager.get_db_config(db_name)
                connection_string = self.build_connection_string(db_config)
                
                # 重試機制
                for attempt in range(self.etl_config.MAX_RETRY_ATTEMPTS):
                    try:
                        connection = pyodbc.connect(connection_string)
                        # 設定連線超時
                        connection.timeout = self.etl_config.COMMAND_TIMEOUT
                        self._connections[db_name] = connection
                        self.logger.info(f"成功連接到 {db_name} 資料庫")
                        break
                    except Exception as e:
                        if attempt < self.etl_config.MAX_RETRY_ATTEMPTS - 1:
                            self.logger.warning(f"連接 {db_name} 失敗，{self.etl_config.RETRY_DELAY_SECONDS}秒後重試... (嘗試 {attempt + 1}/{self.etl_config.MAX_RETRY_ATTEMPTS})")
                            time.sleep(self.etl_config.RETRY_DELAY_SECONDS)
                        else:
                            self.logger.error(f"連接 {db_name} 資料庫失敗: {e}")
                            raise
                            
            except Exception as e:
                self.logger.error(f"建立 {db_name} 資料庫連線失敗: {e}")
                raise
        
        return self._connections[db_name]
    
    def get_engine(self, db_name: str, force_new: bool = False):
        """取得SQLAlchemy引擎"""
        if force_new or db_name not in self._engines:
            try:
                db_config = self.config_manager.get_db_config(db_name)
                uri = self.build_sqlalchemy_uri(db_config)
                
                # 建立引擎
                engine = create_engine(
                    uri,
                    fast_executemany=True,
                    pool_pre_ping=True,  # 連線池預檢查
                    pool_recycle=3600,   # 每小時回收連線
                    echo=False  # 設為True可顯示SQL語句（除錯用）
                )
                
                self._engines[db_name] = engine
                self.logger.info(f"成功創建 {db_name} SQLAlchemy引擎")
                
            except Exception as e:
                self.logger.error(f"創建 {db_name} SQLAlchemy引擎失敗: {e}")
                raise
        
        return self._engines[db_name]
    
    @contextmanager
    def get_connection_context(self, db_name: str):
        """提供資料庫連線的上下文管理器"""
        connection = None
        try:
            connection = self.get_connection(db_name)
            yield connection
        except Exception as e:
            self.logger.error(f"資料庫操作失敗: {e}")
            if connection:
                try:
                    connection.rollback()
                except:
                    pass
            raise
        finally:
            # 注意：這裡不關閉連線，因為要重用
            pass
    
    @contextmanager
    def get_engine_context(self, db_name: str):
        """提供SQLAlchemy引擎的上下文管理器"""
        engine = None
        try:
            engine = self.get_engine(db_name)
            yield engine
        except Exception as e:
            self.logger.error(f"資料庫引擎操作失敗: {e}")
            raise
    
    def execute_query_safely(self, connection: pyodbc.Connection, query: str, params: Dict[str, Any] = None) -> pyodbc.Cursor:
        """安全執行SQL查詢，使用參數化查詢防止SQL注入"""
        try:
            cursor = connection.cursor()
            if params:
                # 使用參數化查詢
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        except Exception as e:
            self.logger.error(f"執行查詢失敗: {e}")
            raise
    
    def execute_query_with_sqlalchemy(self, engine, query: str, params: Dict[str, Any] = None):
        """使用SQLAlchemy安全執行查詢"""
        try:
            with engine.connect() as connection:
                if params:
                    result = connection.execute(text(query), params)
                else:
                    result = connection.execute(text(query))
                return result
        except Exception as e:
            self.logger.error(f"SQLAlchemy查詢執行失敗: {e}")
            raise
    
    def check_table_exists(self, db_name: str, table_name: str) -> bool:
        """安全檢查表是否存在"""
        query = """
        SELECT CASE WHEN EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = ?
        ) THEN 1 ELSE 0 END
        """
        
        try:
            with self.get_engine_context(db_name) as engine:
                result = self.execute_query_with_sqlalchemy(
                    engine, query, {'table_name': table_name}
                )
                return result.scalar() == 1
        except Exception as e:
            self.logger.error(f"檢查表 {table_name} 是否存在失敗: {e}")
            return False
    
    def get_table_structure(self, db_name: str, table_name: str) -> list:
        """取得資料表結構資訊"""
        query = """
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        
        try:
            with self.get_connection_context(db_name) as connection:
                cursor = self.execute_query_safely(connection, query, [table_name])
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
            self.logger.error(f"取得表 {table_name} 結構失敗: {e}")
            return []
    
    def close_connections(self):
        """關閉所有連線"""
        for db_name, connection in self._connections.items():
            try:
                if connection:
                    connection.close()
                    self.logger.info(f"已關閉 {db_name} 資料庫連線")
            except Exception as e:
                self.logger.warning(f"關閉 {db_name} 連線時發生錯誤: {e}")
        
        self._connections.clear()
        
        # 清理SQLAlchemy引擎
        for db_name, engine in self._engines.items():
            try:
                engine.dispose()
                self.logger.info(f"已清理 {db_name} SQLAlchemy引擎")
            except Exception as e:
                self.logger.warning(f"清理 {db_name} 引擎時發生錯誤: {e}")
        
        self._engines.clear()
    
    def test_connection(self, db_name: str) -> bool:
        """測試資料庫連線"""
        try:
            with self.get_connection_context(db_name) as connection:
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    self.logger.info(f"{db_name} 資料庫連線測試成功")
                    return True
                else:
                    self.logger.error(f"{db_name} 資料庫連線測試失敗")
                    return False
        except Exception as e:
            self.logger.error(f"{db_name} 資料庫連線測試失敗: {e}")
            return False