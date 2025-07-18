#!/bin/bash

# ETL系統更新腳本 - 升級版
# 支援新的模組化架構和安全檢查

# 設定常數
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/update_etl.log"
ERROR_LOG_FILE="$SCRIPT_DIR/update_etl_error.log"

# 顏色代碼
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# 日誌函數
log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

error_log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - ${RED}[ERROR]${NC} $1" | tee -a "$ERROR_LOG_FILE"
}

success_log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - ${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

warning_log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - ${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

# 檢查Git狀態
check_git_status() {
    log "檢查Git狀態..."
    
    if [ ! -d ".git" ]; then
        error_log "當前目錄不是Git倉庫"
        return 1
    fi
    
    # 檢查是否有未提交的變更
    if ! git diff --quiet; then
        warning_log "檢測到未提交的變更"
        git status --porcelain | tee -a "$LOG_FILE"
        
        read -p "是否要儲存這些變更？(y/n): " save_changes
        if [ "$save_changes" = "y" ] || [ "$save_changes" = "Y" ]; then
            git stash push -m "Auto-stash before update $(date)"
            log "已儲存未提交的變更"
        fi
    fi
    
    return 0
}

# 更新代碼
update_code() {
    log "開始更新代碼..."
    
    # 記錄更新前的版本
    local current_commit=$(git rev-parse HEAD)
    log "更新前版本: $current_commit"
    
    # 拉取最新代碼
    if git pull origin main; then
        local new_commit=$(git rev-parse HEAD)
        if [ "$current_commit" != "$new_commit" ]; then
            success_log "代碼更新成功: $current_commit -> $new_commit"
            return 0
        else
            log "代碼已是最新版本"
            return 0
        fi
    else
        error_log "代碼更新失敗"
        return 1
    fi
}

# 驗證更新後的代碼
validate_update() {
    log "驗證更新後的代碼..."
    
    # 檢查核心文件是否存在
    local required_files=("app.py" "config.py" "database.py" "sql_loader.py" "diagnose_etl.py")
    
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            error_log "缺少核心文件: $file"
            return 1
        fi
    done
    
    # 檢查Python語法
    log "檢查Python語法..."
    for py_file in *.py; do
        if [ -f "$py_file" ]; then
            if ! python3 -m py_compile "$py_file"; then
                error_log "Python語法錯誤: $py_file"
                return 1
            fi
        fi
    done
    
    # 嘗試導入核心模組
    log "測試核心模組載入..."
    if ! python3 -c "from config import get_config_manager; print('配置模組載入成功')"; then
        error_log "配置模組載入失敗"
        return 1
    fi
    
    success_log "代碼驗證通過"
    return 0
}

# 執行診斷
run_diagnostics() {
    log "執行系統診斷..."
    
    if [ -f "diagnose_etl.py" ]; then
        if python3 diagnose_etl.py --connections-only; then
            success_log "系統診斷通過"
            return 0
        else
            warning_log "系統診斷發現問題，請查看診斷報告"
            return 1
        fi
    else
        warning_log "找不到診斷工具，跳過診斷"
        return 0
    fi
}

# 主函數
main() {
    log "===== ETL系統更新開始 ====="
    
    # 切換到腳本目錄
    cd "$SCRIPT_DIR" || {
        error_log "無法切換到腳本目錄: $SCRIPT_DIR"
        exit 1
    }
    
    # 檢查Git狀態
    if ! check_git_status; then
        error_log "Git狀態檢查失敗"
        exit 1
    fi
    
    # 更新代碼
    if ! update_code; then
        error_log "代碼更新失敗"
        exit 1
    fi
    
    # 驗證更新
    if ! validate_update; then
        error_log "代碼驗證失敗"
        exit 1
    fi
    
    # 執行診斷
    run_diagnostics
    
    log "===== ETL系統更新完成 ====="
    success_log "更新成功！日誌保存於: $LOG_FILE"
    
    exit 0
}

# 執行主函數
main "$@"