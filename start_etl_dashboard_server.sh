#!/bin/bash

# 啟動簡易HTTP服務器來查看ETL儀表板
# 用途: 提供輕量級Web伺服器，無需安裝額外軟件

# 設定變數
SERVER_PORT=8000
ETL_DIR="/home/ETL/etl"
LOG_FILE="/home/ETL/logs/http_server.log"

# 確保日誌目錄存在
mkdir -p $(dirname $LOG_FILE)

# 檢查Python版本
PYTHON_CMD="python3"
if ! command -v $PYTHON_CMD &> /dev/null; then
    PYTHON_CMD="python"
fi

# 停止已存在的伺服器進程
kill_server() {
    echo "檢查已存在的HTTP伺服器進程..."
    OLD_PID=$(ps -ef | grep "$PYTHON_CMD -m http.server $SERVER_PORT" | grep -v grep | awk '{print $2}')
    if [ ! -z "$OLD_PID" ]; then
        echo "發現正在運行的伺服器 (PID: $OLD_PID)，正在終止..."
        kill $OLD_PID
        sleep 2
    fi
}

# 創建索引頁面
create_index() {
    echo "創建索引頁面..."
    cat > $ETL_DIR/dashboard_index.html <<EOL
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>YS ETL 報告中心</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            text-align: center;
            margin-bottom: 20px;
        }
        .section {
            margin-bottom: 30px;
        }
        .btn {
            display: inline-block;
            background-color: #4CAF50;
            color: white;
            padding: 10px 15px;
            text-decoration: none;
            border-radius: 4px;
            margin: 5px;
        }
        .btn:hover {
            background-color: #45a049;
        }
        .status {
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 4px;
            background-color: #e7f3fe;
            border-left: 6px solid #2196F3;
        }
        .footer {
            margin-top: 30px;
            text-align: center;
            font-size: 12px;
            color: #777;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>YS ETL 報告中心</h1>
        
        <div class="status">
            <p><strong>狀態:</strong> 伺服器運行中 (端口: $SERVER_PORT)</p>
            <p><strong>最後更新:</strong> $(date '+%Y-%m-%d %H:%M:%S')</p>
        </div>
        
        <div class="section">
            <h2>儀表板</h2>
            <a href="etl_dashboard.html" class="btn">查看最新儀表板</a>
        </div>
        
        <div class="section">
            <h2>最近報告</h2>
            <ul>
EOL

    # 添加最近的報告文件
    for report in $(find $ETL_DIR -name "etl_report_*.txt" -type f | sort -r | head -5); do
        report_name=$(basename $report)
        report_date=${report_name#etl_report_}
        report_date=${report_date%.txt}
        echo "<li><a href=\"$report_name\">$report_date 報告</a></li>" >> $ETL_DIR/dashboard_index.html
    done

    # 完成索引頁面
    cat >> $ETL_DIR/dashboard_index.html <<EOL
            </ul>
        </div>
        
        <div class="footer">
            <p>YS ETL 監控系統 - 由內部IT團隊提供</p>
        </div>
    </div>
    
    <script>
        // 自動刷新頁面
        setTimeout(function() {
            location.reload();
        }, 300000); // 每5分鐘刷新一次
    </script>
</body>
</html>
EOL

    # 創建符號連結，使首頁指向此索引
    ln -sf $ETL_DIR/dashboard_index.html $ETL_DIR/index.html
}

# 主函數
main() {
    echo "===== ETL儀表板HTTP伺服器 =====" | tee -a $LOG_FILE
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 開始啟動伺服器..." | tee -a $LOG_FILE
    
    # 停止已存在的伺服器
    kill_server
    
    # 創建索引頁面
    create_index
    
    # 切換到ETL目錄
    cd $ETL_DIR
    
    # 啟動HTTP伺服器
    echo "啟動HTTP伺服器在端口 $SERVER_PORT..." | tee -a $LOG_FILE
    nohup $PYTHON_CMD -m http.server $SERVER_PORT > /dev/null 2>&1 &
    
    # 檢查伺服器是否成功啟動
    sleep 2
    NEW_PID=$(ps -ef | grep "$PYTHON_CMD -m http.server $SERVER_PORT" | grep -v grep | awk '{print $2}')
    
    if [ ! -z "$NEW_PID" ]; then
        echo "伺服器已成功啟動! (PID: $NEW_PID)" | tee -a $LOG_FILE
        echo "您可以通過以下地址訪問ETL儀表板:" | tee -a $LOG_FILE
        echo "http://$(hostname -I | awk '{print $1}'):$SERVER_PORT/" | tee -a $LOG_FILE
        echo "或者在局域網內訪問:" | tee -a $LOG_FILE
        echo "http://$(hostname):$SERVER_PORT/" | tee -a $LOG_FILE
    else
        echo "伺服器啟動失敗! 請查看日誌: $LOG_FILE" | tee -a $LOG_FILE
    fi
    
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 操作完成" | tee -a $LOG_FILE
}

# 執行主函數
main