#!/bin/bash

# 簡單的ETL報告生成腳本
# 這個腳本只生成報告，不執行ETL處理

# 設定日誌和報告目錄
REPORT_DIR="./reports"
REPORT_FILE="$REPORT_DIR/etl_report_$(date +%Y%m%d%H%M).txt"

# 確保報告目錄存在
mkdir -p $REPORT_DIR
echo "創建報告目錄: $REPORT_DIR"

# 生成簡單的報告
echo "===== ETL執行報告 - $(date '+%Y-%m-%d %H:%M:%S') =====" > $REPORT_FILE
echo "" >> $REPORT_FILE
echo "主機名稱: $(hostname)" >> $REPORT_FILE
echo "執行用戶: $(whoami)" >> $REPORT_FILE
echo "系統負載: $(uptime)" >> $REPORT_FILE
echo "" >> $REPORT_FILE

echo "===== 系統資訊 =====" >> $REPORT_FILE
echo "磁碟使用狀況:" >> $REPORT_FILE
df -h | grep -v tmpfs >> $REPORT_FILE
echo "" >> $REPORT_FILE

echo "記憶體使用狀況:" >> $REPORT_FILE
free -h >> $REPORT_FILE
echo "" >> $REPORT_FILE

echo "===== ETL日誌摘要 =====" >> $REPORT_FILE
if [ -f "./etl_log.log" ]; then
    tail -n 20 ./etl_log.log >> $REPORT_FILE
else
    echo "找不到ETL日誌文件" >> $REPORT_FILE
fi
echo "" >> $REPORT_FILE

echo "===== ETL監控資訊 =====" >> $REPORT_FILE
# 獲取ETL連線測試摘要
if [ -f "./etl_monitor.log" ]; then
    grep -i "ETL" ./etl_monitor.log | tail -n 10 >> $REPORT_FILE
else
    echo "找不到ETL監控日誌文件" >> $REPORT_FILE
fi

# 生成簡單的HTML報告
HTML_FILE="$REPORT_DIR/etl_report_$(date +%Y%m%d%H%M).html"

cat > $HTML_FILE << EOL
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>ETL 執行報告 - $(date '+%Y-%m-%d %H:%M')</title>
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
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>ETL 執行報告</h1>
            <p><strong>產生時間:</strong> $(date '+%Y-%m-%d %H:%M:%S')</p>
        </div>
        
        <div class="summary">
            <h2>執行摘要</h2>
            <p><strong>主機名稱:</strong> $(hostname)</p>
            <p><strong>執行用戶:</strong> $(whoami)</p>
            <p><strong>系統負載:</strong> $(uptime | sed 's/.*average://g')</p>
        </div>
        
        <h2>系統資訊</h2>
        <div class="system-info">
            <p><strong>磁碟使用狀況:</strong></p>
            <pre>$(df -h | grep -v tmpfs)</pre>
            
            <p><strong>記憶體使用狀況:</strong></p>
            <pre>$(free -h)</pre>
        </div>
        
        <h2>ETL日誌摘要</h2>
        <pre>$(if [ -f "./etl_log.log" ]; then tail -n 20 ./etl_log.log; else echo "找不到ETL日誌文件"; fi)</pre>
        
        <h2>ETL監控資訊</h2>
        <pre>$(if [ -f "./etl_monitor.log" ]; then grep -i "ETL" ./etl_monitor.log | tail -n 10; else echo "找不到ETL監控日誌文件"; fi)</pre>
        
        <div class="footer">
            <p>YS ETL 系統 - 自動生成報告</p>
            <p>如需協助，請聯絡 IT 部門</p>
        </div>
    </div>
</body>
</html>
EOL

echo "報告生成完成:"
echo "- 文字報告: $REPORT_FILE"
echo "- HTML報告: $HTML_FILE"