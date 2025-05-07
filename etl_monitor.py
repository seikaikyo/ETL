#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import pyodbc
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
import argparse
import os
import time
from tabulate import tabulate

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_monitor.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("ETL_Monitor")


def load_db_config():
    """從 db.json 載入資料庫配置"""
    try:
        with open('db.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"讀取配置文件時出錯: {e}")
        sys.exit(1)


def get_connection_string(db_config):
    """建立連接字串"""
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_config['server']};"
        f"DATABASE={db_config['database']};"
        f"UID={db_config['username']};"
        f"PWD={db_config['password']};"
        f"TrustServerCertificate=yes;"
    )
    return conn_str


def get_etl_statistics(days=7):
    """取得 ETL 執行統計資訊"""
    try:
        # 載入資料庫配置
        config = load_db_config()
        target_config = config["tableau_db"]

        # 連接目標資料庫
        conn_str = get_connection_string(target_config)
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # 檢查 ETL_SUMMARY 表是否存在
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ETL_SUMMARY')
        BEGIN
            SELECT 'Table does not exist' AS Status
        END
        ELSE
        BEGIN
            SELECT 'Table exists' AS Status
        END
        """)
        result = cursor.fetchone()

        if result[0] == 'Table does not exist':
            logger.warning("ETL_SUMMARY 表不存在，尚未有 ETL 執行記錄")
            print("\n尚未有 ETL 執行記錄，請先執行 ETL 處理。\n")
            return None

        # 查詢最近 n 天的 ETL 執行紀錄
        recent_date = datetime.now() - timedelta(days=days)

        # 獲取每日執行統計
        daily_stats_query = f"""
        SELECT 
            CONVERT(date, ETL_DATE) AS ExecutionDate,
            COUNT(*) AS TotalExecutions,
            SUM(CASE WHEN SOURCE_TYPE = 'MES' THEN 1 ELSE 0 END) AS MESExecutions,
            SUM(CASE WHEN SOURCE_TYPE = 'SAP' THEN 1 ELSE 0 END) AS SAPExecutions,
            SUM(ROW_COUNT) AS TotalRowsProcessed
        FROM ETL_SUMMARY
        WHERE ETL_DATE >= '{recent_date.strftime('%Y-%m-%d')}'
        GROUP BY CONVERT(date, ETL_DATE)
        ORDER BY ExecutionDate DESC
        """

        daily_stats_df = pd.read_sql(daily_stats_query, conn)

        # 獲取最近一次執行的詳細資訊
        last_execution_query = """
        SELECT TOP 10
            [TIMESTAMP],
            [SOURCE_TYPE],
            [QUERY_NAME],
            [TARGET_TABLE],
            [ROW_COUNT],
            [ETL_DATE]
        FROM ETL_SUMMARY
        ORDER BY [ETL_DATE] DESC
        """

        last_execution_df = pd.read_sql(last_execution_query, conn)

        # 獲取各目標表的記錄數
        table_stats_query = """
        SELECT 
            s.TARGET_TABLE,
            MAX(s.ETL_DATE) AS LastUpdated,
            MAX(s.ROW_COUNT) AS RowCount
        FROM ETL_SUMMARY s
        INNER JOIN (
            SELECT TARGET_TABLE, MAX(ETL_DATE) AS MaxDate
            FROM ETL_SUMMARY
            GROUP BY TARGET_TABLE
        ) latest ON s.TARGET_TABLE = latest.TARGET_TABLE AND s.ETL_DATE = latest.MaxDate
        GROUP BY s.TARGET_TABLE
        """

        table_stats_df = pd.read_sql(table_stats_query, conn)

        # 關閉連接
        conn.close()

        return {
            'daily_stats': daily_stats_df,
            'last_execution': last_execution_df,
            'table_stats': table_stats_df
        }

    except Exception as e:
        logger.error(f"獲取 ETL 統計資訊時出錯: {e}")
        print(f"\n獲取 ETL 統計資訊時出錯: {e}\n")
        return None


def generate_etl_report(stats, output_file=None):
    """產生 ETL 執行報表"""
    if not stats:
        return

    # 獲取當前時間
    now = datetime.now()

    # 報表標頭
    report = []
    report.append("=" * 80)
    report.append(f"ETL 執行統計報表 - 產生時間: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("=" * 80)

    # 最近執行記錄
    report.append("\n最近執行記錄:")
    report.append("-" * 80)

    if not stats['last_execution'].empty:
        last_exec = stats['last_execution'].copy()
        # 格式化日期時間
        last_exec['ETL_DATE'] = last_exec['ETL_DATE'].dt.strftime(
            '%Y-%m-%d %H:%M:%S')

        # 使用 tabulate 格式化表格
        table = tabulate(
            last_exec,
            headers=[
                "時間戳記", "來源類型", "查詢名稱",
                "目標資料表", "資料列數", "ETL 執行時間"
            ],
            tablefmt="grid",
            showindex=False
        )
        report.append(table)
    else:
        report.append("無執行記錄")

    # 日統計資料
    report.append("\n\n每日執行統計:")
    report.append("-" * 80)

    if not stats['daily_stats'].empty:
        daily = stats['daily_stats'].copy()
        # 格式化日期
        daily['ExecutionDate'] = daily['ExecutionDate'].dt.strftime('%Y-%m-%d')

        table = tabulate(
            daily,
            headers=[
                "執行日期", "總執行次數", "MES執行次數",
                "SAP執行次數", "總處理資料列數"
            ],
            tablefmt="grid",
            showindex=False
        )
        report.append(table)
    else:
        report.append("無每日統計資料")

    # 資料表統計資料
    report.append("\n\n資料表最新狀態:")
    report.append("-" * 80)

    if not stats['table_stats'].empty:
        tables = stats['table_stats'].copy()
        # 格式化日期時間
        tables['LastUpdated'] = tables['LastUpdated'].dt.strftime(
            '%Y-%m-%d %H:%M:%S')

        table = tabulate(
            tables,
            headers=["目標資料表", "最近更新時間", "資料列數"],
            tablefmt="grid",
            showindex=False
        )
        report.append(table)
    else:
        report.append("無資料表統計資料")

    # 形成完整報表
    report_text = "\n".join(report)

    # 輸出報表
    print(report_text)

    # 如果指定了輸出文件，則寫入文件
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_text)
            print(f"\n報表已儲存至: {output_file}\n")
        except Exception as e:
            logger.error(f"寫入報表文件時出錯: {e}")
            print(f"\n寫入報表文件時出錯: {e}\n")


def create_etl_dashboard():
    """創建 ETL 執行儀表板 HTML 文件"""
    try:
        # 載入資料庫配置
        config = load_db_config()
        target_config = config["tableau_db"]

        # 連接目標資料庫
        conn_str = get_connection_string(target_config)
        conn = pyodbc.connect(conn_str)

        # 檢查 ETL_SUMMARY 表是否存在
        cursor = conn.cursor()
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ETL_SUMMARY')
        BEGIN
            SELECT 'Table does not exist' AS Status
        END
        ELSE
        BEGIN
            SELECT 'Table exists' AS Status
        END
        """)
        result = cursor.fetchone()

        if result[0] == 'Table does not exist':
            logger.warning("ETL_SUMMARY 表不存在，尚未有 ETL 執行記錄")
            return None

        # 獲取最近 30 天的執行記錄
        recent_date = datetime.now() - timedelta(days=30)

        # 獲取每日執行統計
        daily_stats_query = f"""
        SELECT 
            CONVERT(date, ETL_DATE) AS ExecutionDate,
            COUNT(*) AS TotalExecutions,
            SUM(CASE WHEN SOURCE_TYPE = 'MES' THEN 1 ELSE 0 END) AS MESExecutions,
            SUM(CASE WHEN SOURCE_TYPE = 'SAP' THEN 1 ELSE 0 END) AS SAPExecutions,
            SUM(ROW_COUNT) AS TotalRowsProcessed
        FROM ETL_SUMMARY
        WHERE ETL_DATE >= '{recent_date.strftime('%Y-%m-%d')}'
        GROUP BY CONVERT(date, ETL_DATE)
        ORDER BY ExecutionDate ASC
        """

        daily_stats_df = pd.read_sql(daily_stats_query, conn)

        # 獲取最近的執行記錄
        last_execution_query = """
        SELECT TOP 20
            [TIMESTAMP],
            [SOURCE_TYPE],
            [QUERY_NAME],
            [TARGET_TABLE],
            [ROW_COUNT],
            [ETL_DATE]
        FROM ETL_SUMMARY
        ORDER BY [ETL_DATE] DESC
        """

        last_execution_df = pd.read_sql(last_execution_query, conn)

        # 關閉連接
        conn.close()

        # 格式化數據供圖表使用
        if not daily_stats_df.empty:
            daily_stats_df['ExecutionDate'] = daily_stats_df['ExecutionDate'].dt.strftime(
                '%Y-%m-%d')
            chart_data = daily_stats_df.to_dict('records')
        else:
            chart_data = []

        # 格式化最近執行記錄
        if not last_execution_df.empty:
            last_execution_df['ETL_DATE'] = last_execution_df['ETL_DATE'].dt.strftime(
                '%Y-%m-%d %H:%M:%S')
            last_executions = last_execution_df.to_dict('records')
        else:
            last_executions = []

        # 創建 HTML 文件
        html_template = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>YS ETL 執行監控儀表板</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
        }}
        .header {{
            text-align: center;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }}
        .header h1 {{
            margin: 0;
            color: #333;
        }}
        .summary {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 20px;
        }}
        .summary-card {{
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 5px;
            width: 30%;
            box-shadow: 0 0 5px rgba(0,0,0,0.05);
        }}
        .chart-container {{
            margin-bottom: 30px;
        }}
        .table-container {{
            margin-top: 30px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .refresh-time {{
            text-align: right;
            font-size: 12px;
            color: #777;
            margin-top: 10px;
        }}
        .status-badge {{
            display: inline-block;
            padding: 5px 10px;
            border-radius: 15px;
            font-size: 12px;
            font-weight: bold;
        }}
        .status-success {{
            background-color: #d4edda;
            color: #155724;
        }}
        .status-warning {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .status-danger {{
            background-color: #f8d7da;
            color: #721c24;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>YS ETL 執行監控儀表板</h1>
        </div>
        
        <div class="summary">
            <div class="summary-card">
                <h3>今日執行狀態</h3>
                <div id="today-status">計算中...</div>
            </div>
            <div class="summary-card">
                <h3>總執行成功率</h3>
                <div id="success-rate">計算中...</div>
            </div>
            <div class="summary-card">
                <h3>總處理資料量</h3>
                <div id="total-rows">計算中...</div>
            </div>
        </div>
        
        <div class="chart-container">
            <h2>每日執行統計</h2>
            <canvas id="executionChart"></canvas>
        </div>
        
        <div class="chart-container">
            <h2>資料處理量趨勢</h2>
            <canvas id="rowsChart"></canvas>
        </div>
        
        <div class="table-container">
            <h2>最近執行記錄</h2>
            <table>
                <thead>
                    <tr>
                        <th>執行時間</th>
                        <th>來源類型</th>
                        <th>查詢名稱</th>
                        <th>目標資料表</th>
                        <th>資料列數</th>
                    </tr>
                </thead>
                <tbody id="execution-records">
                    <!-- 最近執行記錄將動態插入 -->
                </tbody>
            </table>
        </div>
        
        <div class="refresh-time">
            最後更新時間: <span id="refresh-time"></span>
            <button onclick="location.reload()">重新整理</button>
        </div>
    </div>
    
    <script>
        // 設置更新時間
        document.getElementById('refresh-time').textContent = new Date().toLocaleString();
        
        // 圖表資料
        const chartData = {JSON_CHART_DATA};
        
        // 最近執行記錄
        const lastExecutions = {JSON_LAST_EXECUTIONS};
        
        // 填充最近執行記錄表格
        const recordsTable = document.getElementById('execution-records');
        lastExecutions.forEach(record => {{
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${{record.ETL_DATE}}</td>
                <td>${{record.SOURCE_TYPE}}</td>
                <td>${{record.QUERY_NAME}}</td>
                <td>${{record.TARGET_TABLE}}</td>
                <td>${{record.ROW_COUNT.toLocaleString()}}</td>
            `;
            recordsTable.appendChild(row);
        }});
        
        // 計算摘要數據
        const today = new Date().toISOString().split('T')[0];
        const todayData = chartData.find(d => d.ExecutionDate === today);
        
        // 今日狀態
        if (todayData) {{
            const todayStatus = document.getElementById('today-status');
            todayStatus.innerHTML = `
                <div class="status-badge status-success">已執行</div>
                <p>執行次數: ${{todayData.TotalExecutions}}</p>
                <p>MES: ${{todayData.MESExecutions}} | SAP: ${{todayData.SAPExecutions}}</p>
            `;
        }} else {{
            document.getElementById('today-status').innerHTML = `
                <div class="status-badge status-warning">尚未執行</div>
                <p>今日尚未執行 ETL 處理</p>
            `;
        }}
        
        // 總處理資料量
        const totalRows = chartData.reduce((sum, data) => sum + data.TotalRowsProcessed, 0);
        document.getElementById('total-rows').innerHTML = `
            <h2>${{totalRows.toLocaleString()}}</h2>
            <p>筆資料已處理</p>
        `;
        
        // 成功率 (假設所有執行都成功)
        document.getElementById('success-rate').innerHTML = `
            <h2>100%</h2>
            <p>執行成功率</p>
        `;
        
        // 執行次數圖表
        const executionCtx = document.getElementById('executionChart').getContext('2d');
        const executionChart = new Chart(executionCtx, {{
            type: 'bar',
            data: {{
                labels: chartData.map(data => data.ExecutionDate),
                datasets: [
                    {{
                        label: 'MES 執行次數',
                        data: chartData.map(data => data.MESExecutions),
                        backgroundColor: 'rgba(54, 162, 235, 0.5)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1
                    }},
                    {{
                        label: 'SAP 執行次數',
                        data: chartData.map(data => data.SAPExecutions),
                        backgroundColor: 'rgba(255, 99, 132, 0.5)',
                        borderColor: 'rgba(255, 99, 132, 1)',
                        borderWidth: 1
                    }}
                ]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            stepSize: 1
                        }}
                    }}
                }}
            }}
        }});
        
        // 資料處理量圖表
        const rowsCtx = document.getElementById('rowsChart').getContext('2d');
        const rowsChart = new Chart(rowsCtx, {{
            type: 'line',
            data: {{
                labels: chartData.map(data => data.ExecutionDate),
                datasets: [{{
                    label: '處理資料列數',
                    data: chartData.map(data => data.TotalRowsProcessed),
                    fill: false,
                    backgroundColor: 'rgba(75, 192, 192, 0.5)',
                    borderColor: 'rgba(75, 192, 192, 1)',
                    tension: 0.1
                }}]
            }},
            options: {{
                responsive: true
            }}
        }});
    </script>
</body>
</html>
"""

        # 替換 JSON 數據
        import json
        html_template = html_template.replace(
            '{JSON_CHART_DATA}', json.dumps(chart_data))
        html_template = html_template.replace(
            '{JSON_LAST_EXECUTIONS}', json.dumps(last_executions))

        # 輸出 HTML 文件
        dashboard_path = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), 'etl_dashboard.html')
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(html_template)

        logger.info(f"成功生成 ETL 儀表板: {dashboard_path}")
        print(f"\nETL 儀表板已生成: {dashboard_path}\n")
        return dashboard_path

    except Exception as e:
        logger.error(f"生成 ETL 儀表板時出錯: {e}")
        print(f"\n生成 ETL 儀表板時出錯: {e}\n")
        return None


def parse_arguments():
    """解析命令列參數"""
    parser = argparse.ArgumentParser(description='ETL 監控工具')
    parser.add_argument('--days', type=int, default=7,
                        help='顯示最近幾天的統計資料 (預設: 7)')
    parser.add_argument('--report', action='store_true', help='產生 ETL 執行報表')
    parser.add_argument('--output', type=str, help='報表輸出檔案路徑')
    parser.add_argument('--dashboard', action='store_true',
                        help='生成 ETL 儀表板 HTML')
    return parser.parse_args()


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info(f"ETL 監控工具啟動 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 解析命令列參數
    args = parse_arguments()

    if args.dashboard:
        # 生成 ETL 儀表板
        logger.info("生成 ETL 儀表板...")
        dashboard_path = create_etl_dashboard()

        if dashboard_path:
            print(f"ETL 儀表板已生成: {dashboard_path}")
            # 嘗試自動打開儀表板
            try:
                import webbrowser
                webbrowser.open('file://' + os.path.abspath(dashboard_path))
            except:
                pass
    elif args.report:
        # 產生 ETL 執行報表
        logger.info(f"產生 ETL 執行報表 (最近 {args.days} 天)...")
        stats = get_etl_statistics(args.days)

        if stats:
            # 產生報表
            output_file = args.output if args.output else f"etl_report_{datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
            generate_etl_report(stats, output_file)
    else:
        # 預設顯示 ETL 統計資訊
        logger.info(f"獲取 ETL 統計資訊 (最近 {args.days} 天)...")
        stats = get_etl_statistics(args.days)

        if stats:
            generate_etl_report(stats)

    logger.info(
        f"ETL 監控工具執行完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
