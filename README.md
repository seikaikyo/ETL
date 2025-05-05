# ETL 工具說明

本專案包含從 MES 系統取得資料並轉換至 Tableau 資料庫的 ETL 工具和相關設定。

## 資料庫連線說明

### 來源資料庫 (MES)

- 伺服器: 10.6.51.147
- 資料庫名稱: yesiang-MES-AP_NEW
- 使用者名稱: Selector
- 密碼: Zl@cvUTg^G!1

### 目標資料庫 (Tableau)

- 伺服器: 10.6.51.202
- 資料庫名稱: TableauDB
- 使用者名稱: TableauDB_user
- 密碼: cGjxug6D2QyMCmThwn8PWt

## 檔案說明

- `app.py` - ETL 連線測試程式，用於驗證來源與目標資料庫的連線
- `db.json` - 資料庫連線設定檔
- `setup.sh` - 環境設置、安裝及測試腳本
- `.gitlab-ci.yml` - GitLab CI/CD 設定檔

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

1. 測試階段 (test_connection): 測試與來源、目標資料庫的連線
2. 部署階段 (deploy_etl): 執行完整環境設置

## 開發注意事項

1. 請勿將 `db.json` 中的資料庫憑證資訊提交到公開倉庫
2. 請確保 VM 環境可以連接到來源資料庫 (10.6.51.147) 和目標資料庫 (10.6.51.202)
3. 若遇到問題，請先查看 `etl_log.log` 和 `setup_log.txt` 日誌文件

## 開發流程

1. 修改本地程式碼
2. 測試資料庫連線: `./setup.sh test_db`
3. 提交程式碼並推送到 GitLab
4. 查看 CI/CD 管道執行結果
5. 如果成功，代碼將自動部署到環境中
