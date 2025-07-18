# ETL 工具說明

本專案包含從多個來源資料庫 (MES、SAP) 取得資料並轉換至 Tableau 資料庫的 ETL 工具和相關設定。

## 資料庫連線說明

連線設定存放於 `db.json` 檔案中，包含所有來源資料庫與目標資料庫的連線資訊。

**注意**: 為了安全考量，敏感連線資訊不應直接寫在文件中，請參考「安全性注意事項」章節。

### 資料庫資訊

- 來源資料庫:
  - MES (10.6.51.147)
  - SAP (10.201.0.37)
- 目標資料庫: Tableau (10.6.51.202)

## 目錄結構

專案檔案組織如下：

```
etl/
├── app.py                   # ETL 主程式
├── db.json                  # 資料庫連線設定檔 (敏感資訊)
├── db.template.json         # 資料庫連線設定範本
├── requirements.txt         # Python 相依套件清單
├── query_metadata.json      # 查詢定義中繼資料
├── mes/                     # MES 相關查詢
│   ├── mes_material_loss.sql  # MES 物料損耗查詢
│   └── mes_order_status.sql   # MES 工單狀態查詢
├── sap/                     # SAP 相關查詢
│   └── sap_production_order.sql  # SAP 生產訂單查詢
├── etl_scheduler.sh         # ETL 排程執行腳本
├── setup.sh                 # 環境設置腳本
└── reports/                 # 報告輸出目錄
    └── etl_latest_report.html  # 最新 ETL 執行報告
```

## 檔案說明

### 核心程式檔案

- `app.py` - ETL 主程式，用於從來源資料庫擷取資料並轉換至目標資料庫
- `etl_scheduler.sh` - ETL 排程執行腳本，設定為每天 16:00 和 00:00 執行
- `etl_monitor.py` - ETL 監控工具，用於生成執行報告與儀表板
- `etl_dashboard.py` - Streamlit 儀表板應用，提供 ETL 執行狀態的可視化介面
- `generate_etl_report.sh` - 簡單的 ETL 報告生成腳本

### 設定與查詢檔案

- `db.json` - 資料庫連線設定檔 (包含敏感資訊)
- `db.template.json` - 資料庫連線設定範本 (不含敏感資訊)
- `query_metadata.json` - 查詢定義中繼資料，包含查詢名稱、目標表及 SQL 檔案路徑
- `mes_queries.json` - MES 資料庫查詢定義
- `sap_queries.json` - SAP 資料庫查詢定義

### SQL 查詢檔案

ETL 系統使用的 SQL 查詢檔案按資料來源分類存放：

- **MES 查詢** (mes/):

  - `mes_material_loss.sql`: 擷取 MES 物料損耗相關資料，包含物料使用量、標準用量與實際用量差異等
  - `mes_order_status.sql`: 擷取 MES 工單狀態相關資料，包含工單進度、預計與實際完工時間比較等

- **SAP 查詢** (sap/):
  - `sap_production_order.sql`: 擷取 SAP 生產訂單相關資料，包含最近三個月的工單資訊

### 環境設置與部署檔案

- `setup.sh` - 環境設置、安裝及測試腳本
- `setup_crontab.sh` - 設置排程任務的腳本
- `update_etl.sh` - 從 Git 倉庫更新 ETL 程式碼的腳本
- `.gitlab-ci.yml` - GitLab CI/CD 設定檔
- `.env.example` - 環境變數設定範本
- `requirements.txt` - Python 相依套件清單

### 診斷與測試工具

- `debug_mes.py` - MES 資料庫資料診斷工具
- `diagnose_mes.py` - MES 詳細診斷工具
- `create_etl_summary.sql` - ETL 摘要表結構建立 SQL

## 相依套件

本專案使用的 Python 套件版本如下：

```
numpy==2.0.2
pandas==2.2.3
pyodbc==5.2.0
python-dateutil==2.9.0.post0
pytz==2025.2
six==1.17.0
tzdata==2025.2
tabulate==0.9.0
```

## 安裝使用方法

### 手動安裝

```bash
# 請確認已克隆此倉庫
git clone https://gitlab.yesiang.com/ys_it_teams/etl.git
cd etl

# 設定執行權限
chmod +x setup.sh

# 安裝完整環境 (包含所有相依套件及 ODBC 驅動程式)
./setup.sh install

# 僅測試資料庫連線
./setup.sh test_db

# 檢查現有環境
./setup.sh check
```

### GitLab CI/CD 自動部署

當有程式碼推送到主分支 (main) 時，GitLab CI/CD 會自動執行以下流程：

1. 測試階段 (test_connection): 測試與所有資料庫的連線
2. 部署階段 (deploy_etl): 執行完整環境設置

## 資料流程

本 ETL 工具支援從多個來源資料庫擷取資料到單一目標資料庫：

1. **MES → Tableau**: 從製造執行系統擷取資料
2. **SAP → Tableau**: 從企業資源規劃系統擷取資料

所有資料最終都會整合到 Tableau 資料庫中，以供資料視覺化和分析使用。

## 排程設定

系統使用 crontab 進行排程，可通過以下方式設定：

```bash
# 設定執行權限
chmod +x setup_crontab.sh

# 執行設定腳本
./setup_crontab.sh
```

預設排程設定為每天 16:00 和 00:00 執行 ETL 處理。可在 `setup_crontab.sh` 中修改排程設定。

手動執行 ETL 處理可使用以下指令：

```bash
# 執行所有 ETL 處理
python app.py --all

# 僅執行 MES 資料
python app.py --mes

# 僅執行 SAP 資料
python app.py --sap

# 啟用詳細除錯訊息
python app.py --all --debug
```

## 監控與報表

ETL 執行後會產生執行報告和監控資訊，可通過以下方式查看：

### 文字報告

```bash
# 生成最近 7 天的 ETL 執行報告
python etl_monitor.py --report --days=7 --output=etl_report.txt

# 生成簡單的 ETL 報告
./generate_etl_report.sh
```

### 監控儀表板

```bash
# 生成 HTML 監控儀表板
python etl_monitor.py --dashboard

# 啟動 Streamlit 互動式儀表板
streamlit run etl_dashboard.py
```

監控儀表板提供以下功能：

- ETL 執行狀態摘要
- 每日執行統計圖表
- 資料處理量趨勢
- 目標資料表最新狀態
- 最近執行記錄詳情

## 安全性注意事項

1. **資料庫憑證管理**:

   - 憑證資訊存放於 `db.json` 檔案中，此檔案已加入 `.gitignore`
   - 請勿將包含敏感資訊的 `db.json` 提交至版本控制系統
   - 開發人員請使用 `db.template.json` 做為範本，自行建立 `db.json` 並填入連線資訊

2. **環境變數使用建議**:

   - 在生產環境中，應使用環境變數或安全的憑證管理服務存放敏感資訊
   - 可參考 `.env.example` 檔案設定所需的環境變數

3. **GitLab CI/CD 變數**:
   - 在 GitLab CI/CD 中，可使用保護變數存放敏感資訊
   - 設定路徑: Settings > CI/CD > Variables

## 疑難排解

若遇到問題，請先查看下列日誌檔案:

- `etl_log.log`: ETL 執行日誌
- `setup_log.txt`: 環境設置日誌
- `etl_monitor.log`: ETL 監控執行日誌
- `mes_diagnostic_*.log`: MES 診斷日誌

### 資料庫連線問題

1. **資料庫連線失敗**:

   - 確認資料庫伺服器是否可連線 (使用 ping 或 telnet 測試)
   - 確認資料庫憑證是否正確
   - 檢查 ODBC 驅動程式是否正確安裝
   - 確認防火牆設定允許連接到資料庫伺服器

2. **SAP 連線問題**:
   - 確認網路設定允許連接到 SAP 伺服器
   - 確認資料庫名稱是否正確 (目前設定為 YS_DB_PRD)
   - 確認使用者帳號擁有足夠的查詢權限

### 資料處理問題

1. **查詢執行失敗**:

   - 可使用 `debug_mes.py` 或 `diagnose_mes.py` 診斷資料問題
   - 檢查查詢結果是否包含 NULL 值或異常資料
   - 可使用 `app.py --debug` 啟用詳細除錯訊息

2. **報告生成問題**:
   - 確認目標資料庫的 ETL_SUMMARY 表結構是否正確
   - 使用 `etl_monitor.py --init` 初始化監控環境

### 排程執行問題

1. **排程不自動執行**:

   - 檢查 crontab 設定是否正確 (使用 `crontab -l` 查看)
   - 確認 `etl_scheduler.sh` 擁有執行權限
   - 檢查系統日誌確認 cron 服務是否正常執行

2. **排程執行但 ETL 失敗**:
   - 查看 `/home/ETL/logs/` 目錄下的日誌檔案
   - 確認腳本中的路徑設定是否正確
   - 檢查 Python 虛擬環境是否正確啟動

## 開發流程

1. 修改本地程式碼
2. 測試資料庫連線: `./setup.sh test_db`
3. 提交程式碼並推送到 GitLab
4. 查看 CI/CD 管道執行結果
5. 如果成功，代碼將自動部署到環境中

## 系統更新維護

使用以下命令更新系統：

```bash
# 從 Git 倉庫更新代碼
./update_etl.sh

# 重置 ETL 監控環境 (需要時)
python etl_monitor.py --init
```

### 增加或修改 SQL 查詢

如需新增或修改 SQL 查詢，請按以下步驟：

1. 依據資料來源在 `mes/` 或 `sap/` 目錄中創建或修改 SQL 檔案
2. 在 `query_metadata.json` 中新增或更新查詢定義，包含 SQL 檔案路徑和目標表
3. 測試 SQL 查詢：

   ```bash
   # 對 MES 查詢進行診斷測試
   python diagnose_mes.py --sql mes/your_new_query.sql

   # 運行 ETL 處理以測試新查詢
   python app.py --all --debug
   ```
