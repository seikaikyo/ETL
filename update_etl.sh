#!/bin/bash

# 設定日誌檔案
NORMAL_LOG_FILE="/home/ETL/git_pull_normal.log"
ERROR_LOG_FILE="/home/ETL/git_pull_error.log"
MAX_NORMAL_LOG_SIZE=1048576  # 1MB 

# 切換到專案目錄
cd /home/ETL/etl

# 配置 git 忽略 SSL 驗證 (如果需要)
git config --global http.sslVerify false

# 創建臨時檔案來捕獲輸出
TEMP_LOG=$(mktemp)

# 記錄時間
echo "===== 開始更新 $(date) =====" > $TEMP_LOG

# 拉取最新代碼
git pull origin main >> $TEMP_LOG 2>&1
PULL_STATUS=$?

# 完成記錄
echo "===== 更新結束 $(date) =====" >> $TEMP_LOG
echo "" >> $TEMP_LOG

# 根據執行結果保存到不同的日誌檔案
if [ $PULL_STATUS -eq 0 ]; then
    # 正常執行 - 檢查正常日誌大小並管理
    if [ -f "$NORMAL_LOG_FILE" ]; then
        log_size=$(stat -c%s "$NORMAL_LOG_FILE" 2>/dev/null || echo "0")
        if [ $log_size -gt $MAX_NORMAL_LOG_SIZE ]; then
            # 保留最後 5 筆記錄
            tail -n 100 "$NORMAL_LOG_FILE" > "${NORMAL_LOG_FILE}.tmp"
            mv "${NORMAL_LOG_FILE}.tmp" "$NORMAL_LOG_FILE"
        fi
    fi
    
    # 添加新的日誌條目
    cat $TEMP_LOG >> $NORMAL_LOG_FILE
    
    # 執行額外的成功操作，如有需要
    # python app.py >> $NORMAL_LOG_FILE 2>&1
else
    # 異常執行 - 保存詳細錯誤日誌，包含時間戳記
    ERROR_TIME=$(date +"%Y%m%d%H%M%S")
    cat $TEMP_LOG > "${ERROR_LOG_FILE}.${ERROR_TIME}"
    
    # 同時追加到主要錯誤日誌
    cat $TEMP_LOG >> $ERROR_LOG_FILE
    
    # 發送通知或其他異常處理，如有需要
    # echo "Git pull 失敗，請查看日誌 ${ERROR_LOG_FILE}.${ERROR_TIME}" | mail -s "ETL 更新失敗" admin@example.com
fi

# 清理臨時檔案
rm $TEMP_LOG

# 保留最近 30 天的錯誤日誌，刪除更舊的
find /home/ETL -name "git_pull_error.log.*" -mtime +30 -delete