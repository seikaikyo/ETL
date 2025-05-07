#!/bin/bash

# ETL 排程執行腳本
# 用途: 在指定時間執行 ETL 處理

# 設定日誌
LOG_DIR="/home/ETL/logs"
LOG_FILE="${LOG_DIR}/etl_$(date +%Y%m%d).log"

# 確保日誌目錄存在
mkdir -p $LOG_DIR

# 記錄開始時間
echo "===== ETL 排程啟動 - $(date '+%Y-%m-%d %H:%M:%S') =====" >> $LOG_FILE

# 切換到 ETL 專案目錄
cd /home/ETL/etl

# 確保虛擬環境存在
if [ ! -d "venv" ]; then
    echo "建立虛擬環境..." >> $LOG_FILE
    python3 -m venv venv
fi

# 啟動虛擬環境
source venv/bin/activate

# 確保安裝所需套件
pip install -r requirements.txt >> $LOG_FILE 2>&1

# 執行 ETL 處理 (所有來源)
echo "開始執行 ETL 處理..." >> $LOG_FILE
python app.py --all >> $LOG_FILE 2>&1
ETL_STATUS=$?

# 檢查執行結果
if [ $ETL_STATUS -eq 0 ]; then
    echo "ETL 處理成功完成！" >> $LOG_FILE
else
    echo "ETL 處理失敗，錯誤碼: $ETL_STATUS" >> $LOG_FILE
    
    # 可以在這裡加入失敗通知機制，例如寄送郵件
    # mail -s "ETL 處理失敗通知" admin@yesiang.com < $LOG_FILE
fi

# 若日誌檔案過多，清理舊檔
find $LOG_DIR -name "etl_*.log" -mtime +30 -delete

# 記錄結束時間
echo "===== ETL 排程結束 - $(date '+%Y-%m-%d %H:%M:%S') =====" >> $LOG_FILE
echo "" >> $LOG_FILE

# 退出虛擬環境
deactivate
