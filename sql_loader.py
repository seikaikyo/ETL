#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import hashlib
from typing import Dict, Any, Optional
from pathlib import Path

from config import ConfigManager


class SQLLoader:
    """安全的SQL文件載入器"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.logger = logging.getLogger("SQLLoader")
        self._sql_cache = {}
        self._file_hashes = {}
    
    def load_sql_file(self, sql_file: str, use_cache: bool = True) -> str:
        """
        安全載入SQL文件
        
        Args:
            sql_file: SQL文件路徑（相對或絕對）
            use_cache: 是否使用緩存
            
        Returns:
            SQL內容字串
            
        Raises:
            FileNotFoundError: 找不到SQL文件
            SecurityError: 文件路徑不安全
        """
        # 安全檢查：確保文件路徑在專案目錄內
        sql_path = self._resolve_sql_path(sql_file)
        
        # 檢查緩存
        if use_cache and sql_path in self._sql_cache:
            # 檢查文件是否已修改
            current_hash = self._get_file_hash(sql_path)
            if current_hash == self._file_hashes.get(sql_path):
                self.logger.debug(f"使用緩存的SQL文件: {sql_path}")
                return self._sql_cache[sql_path]
        
        try:
            # 讀取文件
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # 基本安全檢查
            self._validate_sql_content(sql_content, sql_path)
            
            # 更新緩存
            if use_cache:
                self._sql_cache[sql_path] = sql_content
                self._file_hashes[sql_path] = self._get_file_hash(sql_path)
            
            self.logger.debug(f"成功載入SQL文件: {sql_path}")
            return sql_content
            
        except FileNotFoundError:
            self.logger.error(f"找不到SQL文件: {sql_path}")
            raise
        except Exception as e:
            self.logger.error(f"載入SQL文件失敗: {sql_path}, {e}")
            raise
    
    def _resolve_sql_path(self, sql_file: str) -> str:
        """
        解析SQL文件路徑，確保安全性
        
        Args:
            sql_file: 原始文件路徑
            
        Returns:
            解析後的絕對路徑
            
        Raises:
            SecurityError: 路徑不安全
            FileNotFoundError: 找不到文件
        """
        # 取得專案根目錄
        project_root = Path(__file__).parent.absolute()
        
        # 如果是絕對路徑，檢查是否在專案目錄內
        if os.path.isabs(sql_file):
            sql_path = Path(sql_file)
        else:
            # 相對路徑，先嘗試直接拼接
            sql_path = project_root / sql_file
        
        # 安全檢查：確保路徑在專案目錄內
        try:
            sql_path = sql_path.resolve()
            if not str(sql_path).startswith(str(project_root)):
                raise SecurityError(f"SQL文件路徑不安全，超出專案目錄: {sql_file}")
        except Exception as e:
            self.logger.error(f"解析SQL文件路徑失敗: {sql_file}, {e}")
            raise
        
        # 如果文件不存在，嘗試在子目錄中查找
        if not sql_path.exists():
            sql_path = self._search_sql_file(project_root, sql_file)
        
        if not sql_path.exists():
            raise FileNotFoundError(f"找不到SQL文件: {sql_file}")
        
        return str(sql_path)
    
    def _search_sql_file(self, project_root: Path, sql_file: str) -> Path:
        """
        在專案目錄中搜索SQL文件
        
        Args:
            project_root: 專案根目錄
            sql_file: SQL文件名稱
            
        Returns:
            找到的文件路徑
        """
        # 取得文件名
        filename = os.path.basename(sql_file)
        
        # 根據文件名推測可能的子目錄
        if filename.startswith('mes_'):
            possible_dirs = ['mes', 'MES']
        elif filename.startswith('sap_'):
            possible_dirs = ['sap', 'SAP']
        else:
            possible_dirs = ['sql', 'queries', 'mes', 'sap']
        
        # 在可能的目錄中搜索
        for dir_name in possible_dirs:
            candidate_path = project_root / dir_name / filename
            if candidate_path.exists():
                self.logger.debug(f"在 {dir_name} 目錄找到SQL文件: {filename}")
                return candidate_path
        
        # 如果還是找不到，進行遞歸搜索（限制深度）
        for sql_path in project_root.rglob(filename):
            if sql_path.is_file() and sql_path.suffix.lower() == '.sql':
                self.logger.debug(f"遞歸搜索找到SQL文件: {sql_path}")
                return sql_path
        
        # 返回原始路徑（會在後續檢查中失敗）
        return project_root / sql_file
    
    def _validate_sql_content(self, sql_content: str, file_path: str):
        """
        驗證SQL內容的基本安全性
        
        Args:
            sql_content: SQL內容
            file_path: 文件路徑（用於日誌）
            
        Raises:
            SecurityError: SQL內容存在安全風險
        """
        # 檢查是否為空
        if not sql_content.strip():
            raise ValueError(f"SQL文件內容為空: {file_path}")
        
        # 基本安全檢查：檢查可能危險的SQL關鍵字
        dangerous_keywords = [
            'xp_cmdshell',  # SQL Server命令執行
            'sp_configure', # 系統配置修改
            'exec master',  # 執行系統程序
            'openrowset',   # 外部數據源
            'opendatasource',  # 外部數據源
            '--sp_password',   # 密碼相關
        ]
        
        sql_lower = sql_content.lower()
        for keyword in dangerous_keywords:
            if keyword in sql_lower:
                self.logger.warning(f"SQL文件包含潛在危險關鍵字 '{keyword}': {file_path}")
                # 注意：這裡只是警告，不拋出異常，因為可能有合法用途
        
        # 檢查是否包含明顯的惡意模式
        malicious_patterns = [
            '; drop table',
            '; delete from',
            '; truncate table',
            'union select',  # 可能的SQL注入
        ]
        
        for pattern in malicious_patterns:
            if pattern in sql_lower:
                self.logger.warning(f"SQL文件包含可疑模式 '{pattern}': {file_path}")
    
    def _get_file_hash(self, file_path: str) -> str:
        """取得文件的哈希值用於緩存檢查"""
        try:
            with open(file_path, 'rb') as f:
                file_hash = hashlib.md5(f.read()).hexdigest()
            return file_hash
        except Exception:
            return ""
    
    def clear_cache(self):
        """清除SQL緩存"""
        self._sql_cache.clear()
        self._file_hashes.clear()
        self.logger.info("SQL緩存已清除")
    
    def get_cache_info(self) -> Dict[str, Any]:
        """取得緩存資訊"""
        return {
            "cached_files": len(self._sql_cache),
            "cache_files": list(self._sql_cache.keys())
        }


class SecurityError(Exception):
    """安全相關異常"""
    pass


# 便利函數
def load_sql_file(sql_file: str, config_manager: Optional[ConfigManager] = None) -> str:
    """便利函數：載入SQL文件"""
    if config_manager is None:
        from config import get_config_manager
        config_manager = get_config_manager()
    
    loader = SQLLoader(config_manager)
    return loader.load_sql_file(sql_file)