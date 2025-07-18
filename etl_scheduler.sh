#!/bin/bash

# ETL 排程執行腳本
# 用途: 在指定時間執行 ETL 處理並產生執行報告

# 設定日誌和報告目錄
LOG_DIR="/home/ETL/logs"
REPORT_DIR="/home/ETL/reports"
LOG_FILE="${LOG_DIR}/etl_$(date +%Y%m%d).log"
REPORT_FILE="${REPORT_DIR}/etl_report_$(date +%Y%m%d%H%M).txt"
LATEST_REPORT="${REPORT_DIR}/etl_latest_report.txt"
HTML_REPORT="${REPORT_DIR}/etl_report_$(date +%Y%m%d%H%M).html"
LATEST_HTML="${REPORT_DIR}/etl_latest_report.html"

# 確保目錄存在
mkdir -p $LOG_DIR
mkdir -p $REPORT_DIR

# 檢查目錄是否創建成功
if [ ! -d "$LOG_DIR" ]; then
    echo "錯誤: 無法創建日誌目錄 $LOG_DIR"
    exit 1
fi

if [ ! -d "$REPORT_DIR" ]; then
    echo "錯誤: 無法創建報告目錄 $REPORT_DIR"
    exit 1
fi

# 輸出目錄結構，用於調試
echo "目錄結構:"
echo "- 當前目錄: $(pwd)"
echo "- 日誌目錄: $LOG_DIR"
echo "- 報告目錄: $REPORT_DIR"

# 記錄開始時間
echo "===== ETL 排程啟動 - $(date '+%Y-%m-%d %H:%M:%S') =====" | tee -a $LOG_FILE

# 切換到 ETL 專案目錄
cd /home/ETL/etl

# 確保虛擬環境存在
if [ ! -d "venv" ]; then
    echo "建立虛擬環境..." | tee -a $LOG_FILE
    python3 -m venv venv
fi

# 啟動虛擬環境
source venv/bin/activate

# 確保安裝所需套件
pip install -r requirements.txt >> $LOG_FILE 2>&1

# 執行 ETL 處理 (所有來源)
echo "開始執行 ETL 處理..." | tee -a $LOG_FILE
python app.py --all >> $LOG_FILE 2>&1
ETL_STATUS=$?

# 檢查執行結果
if [ $ETL_STATUS -eq 0 ]; then
    echo "ETL 處理成功完成！" | tee -a $LOG_FILE
    ETL_RESULT="成功"
    STATUS_CLASS="success"
else
    echo "ETL 處理失敗，錯誤碼: $ETL_STATUS" | tee -a $LOG_FILE
    ETL_RESULT="失敗"
    STATUS_CLASS="error"
fi

# 產生 ETL 監控報告
echo "產生 ETL 執行報告..." | tee -a $LOG_FILE
python etl_monitor.py --report --output=$REPORT_FILE >> $LOG_FILE 2>&1

# 檢查報告是否生成
if [ ! -f "$REPORT_FILE" ]; then
    echo "警告: 無法生成報告文件 $REPORT_FILE, 嘗試使用相對路徑..." | tee -a $LOG_FILE
    # 嘗試使用相對路徑
    REPORT_FILE="./reports/etl_report_$(date +%Y%m%d%H%M).txt"
    mkdir -p ./reports
    python etl_monitor.py --report --output=$REPORT_FILE >> $LOG_FILE 2>&1
fi

# 再次檢查報告是否生成
if [ ! -f "$REPORT_FILE" ]; then
    echo "錯誤: 無法生成報告文件, 跳過後續步驟" | tee -a $LOG_FILE
    deactivate
    exit 1
fi

# 更新最新報告的連結
echo "更新最新報告連結..." | tee -a $LOG_FILE
cp $REPORT_FILE $LATEST_REPORT || echo "無法複製到 $LATEST_REPORT" | tee -a $LOG_FILE

# 產生 HTML 報告
echo "產生 HTML 格式報告..." | tee -a $LOG_FILE

# 儲存當前時間和主機名到變數
CURRENT_TIME=$(date '+%Y-%m-%d %H:%M:%S')
HOST_NAME=$(hostname)
UPTIME_INFO=$(uptime | sed 's/.*average://g')
USER_NAME=$(whoami)
DISK_INFO=$(df -h | grep -v tmpfs)
MEM_INFO=$(free -h)
REPORT_CONTENT=$(cat $REPORT_FILE 2>/dev/null || echo "無法讀取報告文件 $REPORT_FILE")

# 創建 HTML 報告頭部
cat > $HTML_REPORT << EOF
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>ETL 執行報告 - $CURRENT_TIME</title>
    <style>
        body { 
            font-family: Arial, sans-serif; 
            margin: 0; 
            padding: 20px; 
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1000px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        h1, h2 { 
            color: #333; 
            border-bottom: 1px solid #eee;
            padding-bottom: 10px;
        }
        .header {
            margin-bottom: 20px;
        }
        .summary {
            background-color: #f9f9f9;
            padding: 15px;
            border-left: 5px solid #4CAF50;
            margin-bottom: 20px;
        }
        .summary.error {
            border-left-color: #f44336;
        }
        pre {
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 5px;
            overflow: auto;
            font-family: Consolas, monospace;
            font-size: 14px;
        }
        .footer {
            margin-top: 30px;
            text-align: center;
            font-size: 12px;
            color: #777;
            border-top: 1px solid #eee;
            padding-top: 10px;
        }
        .stats-table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        .stats-table th, .stats-table td {
            padding: 10px;
            border: 1px solid #ddd;
            text-align: left;
        }
        .stats-table th {
            background-color: #f2f2f2;
        }
        .stats-table tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        .status-badge {
            display: inline-block;
            padding: 5px 10px;
            border-radius: 3px;
            font-weight: bold;
            color: white;
        }
        .status-success {
            background-color: #4CAF50;
        }
        .status-error {
            background-color: #f44336;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>ETL 執行報告</h1>
            <p><strong>產生時間:</strong> $CURRENT_TIME</p>
        </div>
        
        <div class="summary ${STATUS_CLASS}">
            <h2>執行摘要</h2>
            <p><strong>執行狀態:</strong> 
                <span class="status-badge status-${STATUS_CLASS}">
                    $ETL_RESULT
                </span>
            </p>
            <p><strong>主機名稱:</strong> $HOST_NAME</p>
            <p><strong>執行時間:</strong> $CURRENT_TIME</p>
            <p><strong>執行用戶:</strong> $USER_NAME</p>
            <p><strong>系統負載:</strong> $UPTIME_INFO</p>
        </div>
        
        <h2>系統資訊</h2>
        <div class="system-info">
            <p><strong>磁碟使用狀況:</strong></p>
            <pre>$DISK_INFO</pre>
            
            <p><strong>記憶體使用狀況:</strong></p>
            <pre>$MEM_INFO</pre>
        </div>
        
        <h2>ETL 執行詳細資訊</h2>
        <pre>$REPORT_CONTENT</pre>
        
        <div class="footer">
            <p>YS ETL 系統 - 自動生成報告</p>
            <p>如需協助，請聯絡 IT 部門</p>
        </div>
    </div>
</body>
</html>
EOF

# 檢查 HTML 報告是否生成
if [ ! -f "$HTML_REPORT" ]; then
    echo "警告: 無法生成 HTML 報告 $HTML_REPORT" | tee -a $LOG_FILE
else
    # 更新最新HTML報告的連結
    cp $HTML_REPORT $LATEST_HTML || echo "無法複製到 $LATEST_HTML" | tee -a $LOG_FILE
fi

# 產生儀表板 (如需要)
echo "更新 ETL 監控儀表板..." | tee -a $LOG_FILE
python etl_monitor.py --dashboard >> $LOG_FILE 2>&1

# 顯示報告位置
echo "ETL執行報告已生成:" | tee -a $LOG_FILE
if [ -f "$REPORT_FILE" ]; then
    echo "- 文字報告: $REPORT_FILE" | tee -a $LOG_FILE
fi
if [ -f "$HTML_REPORT" ]; then
    echo "- HTML報告: $HTML_REPORT" | tee -a $LOG_FILE
fi
if [ -f "$LATEST_REPORT" ]; then
    echo "- 最新報告連結: $LATEST_REPORT" | tee -a $LOG_FILE
fi
if [ -f "$LATEST_HTML" ]; then
    echo "- 最新HTML連結: $LATEST_HTML" | tee -a $LOG_FILE
fi

# 報告位置文件 (方便查找) - 使用相對或絕對路徑，取決於哪個可用
REPORT_LOG_FILE="${REPORT_DIR}/report_locations.log"
if [ ! -w "$(dirname "$REPORT_LOG_FILE")" ]; then
    REPORT_LOG_FILE="./report_locations.log"
fi
echo "$(date '+%Y-%m-%d %H:%M:%S') - ETL執行 ${ETL_RESULT} - 報告位置: $REPORT_FILE, $HTML_REPORT" >> $REPORT_LOG_FILE

# 若日誌檔案過多，清理舊檔
if [ -d "$LOG_DIR" ]; then
    find $LOG_DIR -name "etl_*.log" -mtime +30 -delete
fi
if [ -d "$REPORT_DIR" ]; then
    find $REPORT_DIR -name "etl_report_*.txt" -mtime +30 -delete
    find $REPORT_DIR -name "etl_report_*.html" -mtime +30 -delete
fi

# 記錄結束時間
echo "===== ETL 排程結束 - $(date '+%Y-%m-%d %H:%M:%S') =====" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

# 退出虛擬環境
deactivate