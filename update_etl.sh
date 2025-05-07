#!/bin/bash

# 設定日誌檔案
LOG_FILE="/home/ETL/git_pull.log"

# 記錄時間
echo "===== 開始更新 $(date) =====" >> $LOG_FILE

# 切換到專案目錄
cd /home/ETL/etl

# 配置 git 忽略 SSL 驗證 (如果需要)
git config --global http.sslVerify false

# 拉取最新代碼
git pull origin main >> $LOG_FILE 2>&1

# 如果更新成功，執行測試
if [ $? -eq 0 ]; then
    echo "代碼更新成功，執行測試..." >> $LOG_FILE
    # 可選：自動執行測試
    # python app.py >> $LOG_FILE 2>&1
else
    echo "代碼更新失敗，請檢查錯誤" >> $LOG_FILE
fi

echo "===== 更新結束 $(date) =====" >> $LOG_FILE
echo "" >> $LOG_FILE