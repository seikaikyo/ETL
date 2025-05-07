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

## 檔案說明

- `app.py` - ETL 連線測試程式，用於驗證來源與目標資料庫的連線
- `db.json` - 資料庫連線設定檔 (包含敏感資訊)
- `db.template.json` - 資料庫連線設定範本 (不含敏感資訊)
- `setup.sh` - 環境設置、安裝及測試腳本
- `.gitlab-ci.yml` - GitLab CI/CD 設定檔
- `.env.example` - 環境變數設定範本

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

### 常見問題

1. **資料庫連線失敗**:

   - 確認資料庫伺服器是否可連線 (使用 ping 或 telnet 測試)
   - 確認資料庫憑證是否正確
   - 檢查 ODBC 驅動程式是否正確安裝

2. **SAP 連線問題**:
   - 確認網路設定允許連接到 SAP 伺服器
   - 確認資料庫名稱是否正確 (目前設定為 YS_DB_PRD)

## 開發流程

1. 修改本地程式碼
2. 測試資料庫連線: `./setup.sh test_db`
3. 提交程式碼並推送到 GitLab
4. 查看 CI/CD 管道執行結果
5. 如果成功，代碼將自動部署到環境中
