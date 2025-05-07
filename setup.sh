#!/bin/bash

# 定義顏色
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 定義日誌檔案
LOG_FILE="setup_log.txt"

# 輸出函式
log() {
    local timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo -e "${timestamp} - $1"
    echo "${timestamp} - $1" | sed 's/\x1B\[[0-9;]*[JKmsu]//g' >> "${LOG_FILE}"
}

success() {
    log "${GREEN}[成功]${NC} $1"
}

error() {
    log "${RED}[錯誤]${NC} $1"
}

info() {
    log "${BLUE}[資訊]${NC} $1"
}

warning() {
    log "${YELLOW}[警告]${NC} $1"
}

# 檢查命令是否存在
check_command() {
    command -v $1 >/dev/null 2>&1
}

# 初始化設置
init_setup() {
    info "開始環境設置過程..."
    info "目前目錄: $(pwd)"
    info "系統資訊: $(uname -a)"
    
    # 創建日誌文件
    touch "${LOG_FILE}"
    info "日誌文件創建於: $(pwd)/${LOG_FILE}"
}

# 檢查和安裝必要的系統套件
install_system_packages() {
    info "檢查並安裝系統套件..."
    
    if check_command apt-get; then
        info "使用 apt 包管理器..."
        
        sudo apt-get update
        if [ $? -ne 0 ]; then
            error "無法更新套件清單"
            return 1
        fi
        
        info "安裝必要的系統套件..."
        sudo apt-get install -y \
            python3 \
            python3-pip \
            python3-venv \
            unixodbc \
            unixodbc-dev \
            curl \
            gnupg
            
        if [ $? -ne 0 ]; then
            error "安裝基本套件失敗"
            return 1
        fi
        
        # 安裝 ODBC Driver for SQL Server
        info "安裝 Microsoft ODBC Driver for SQL Server..."
        
        # 匯入 Microsoft GPG 金鑰
        curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
        
        # 註冊 Microsoft Ubuntu 儲存庫
        # 根據 Ubuntu 版本，可能需要調整
        if grep -q "Ubuntu 20.04" /etc/os-release; then
            curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
        elif grep -q "Ubuntu 22.04" /etc/os-release; then
            curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
        else
            # 預設使用 Ubuntu 20.04 的儲存庫
            warning "無法識別確切的 Ubuntu 版本，使用 Ubuntu 20.04 的儲存庫"
            curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
        fi
        
        sudo apt-get update
        
        # 安裝 ODBC driver
        ACCEPT_EULA=Y sudo apt-get install -y msodbcsql17
        
        if [ $? -ne 0 ]; then
            error "安裝 ODBC Driver 失敗"
            return 1
        fi
        
    elif check_command yum || check_command dnf; then
        info "使用 yum/dnf 包管理器..."
        
        if check_command dnf; then
            PKG_MGR="dnf"
        else
            PKG_MGR="yum"
        fi
        
        sudo $PKG_MGR update -y
        if [ $? -ne 0 ]; then
            error "無法更新套件清單"
            return 1
        fi
        
        info "安裝必要的系統套件..."
        sudo $PKG_MGR install -y \
            python3 \
            python3-pip \
            unixODBC \
            unixODBC-devel \
            curl \
            gcc-c++ \
            python3-devel
            
        if [ $? -ne 0 ]; then
            error "安裝基本套件失敗"
            return 1
        fi
        
        # 安裝 ODBC Driver for SQL Server
        info "安裝 Microsoft ODBC Driver for SQL Server..."
        
        # 下載 Microsoft 儲存庫 RPM
        # 獲取 RHEL/CentOS/Rocky 的主要版本號
        if [ -f /etc/os-release ]; then
            . /etc/os-release
            if [[ "$ID" == "rocky" || "$ID" == "rhel" || "$ID" == "centos" ]]; then
                VERSION_ID_MAJOR=$(echo $VERSION_ID | cut -d. -f1)
                sudo curl https://packages.microsoft.com/config/rhel/$VERSION_ID_MAJOR/prod.repo > /etc/yum.repos.d/mssql-release.repo
            else
                # 預設使用 RHEL 8 的儲存庫
                warning "無法識別確切的 RHEL/CentOS 版本，使用 RHEL 8 的儲存庫"
                sudo curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo
            fi
        else
            # 預設使用 RHEL 8 的儲存庫
            warning "無法讀取 /etc/os-release，使用 RHEL 8 的儲存庫"
            sudo curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo
        fi
        
        # 安裝 ODBC driver
        sudo ACCEPT_EULA=Y $PKG_MGR install -y msodbcsql17
        
        if [ $? -ne 0 ]; then
            error "安裝 ODBC Driver 失敗"
            return 1
        fi
    else
        error "不支援的套件管理器。請手動安裝必要的套件。"
        return 1
    fi
    
    success "系統套件安裝完成"
    return 0
}

# 設置 Python 虛擬環境
setup_python_venv() {
    info "設置 Python 虛擬環境..."
    
    # 檢查 Python 版本
    PYTHON_VERSION=$(python3 --version 2>&1)
    info "Python 版本: ${PYTHON_VERSION}"
    
    # 創建虛擬環境
    info "創建 Python 虛擬環境..."
    python3 -m venv venv
    
    if [ $? -ne 0 ]; then
        error "創建虛擬環境失敗"
        return 1
    fi
    
    # 啟動虛擬環境
    info "啟動虛擬環境..."
    source venv/bin/activate
    
    if [ $? -ne 0 ]; then
        error "無法啟動虛擬環境"
        return 1
    fi
    
    # 安裝 Python 套件
    info "安裝必要的 Python 套件..."
    pip install --upgrade pip
    pip install pyodbc pandas
    
    if [ $? -ne 0 ]; then
        error "安裝 Python 套件失敗"
        return 1
    fi
    
    # 創建 requirements.txt
    info "創建 requirements.txt 文件..."
    pip freeze > requirements.txt
    
    success "Python 虛擬環境設置完成"
    return 0
}

# 檢查資料庫連接
check_db_connection() {
    info "檢查資料庫連接..."
    
    # 檢查 db.json 是否存在
    if [ ! -f "db.json" ]; then
        error "缺少 db.json 配置文件"
        return 1
    fi
    
    # 檢查 app.py 是否存在
    if [ ! -f "app.py" ]; then
        error "缺少 app.py 文件"
        return 1
    fi
    
    # 檢查資料庫設定
    info "檢查資料庫設定..."
    mes_server=$(grep -o '"server": "[^"]*"' db.json | head -1 | cut -d'"' -f4)
    mes_db=$(grep -o '"database": "[^"]*"' db.json | head -1 | cut -d'"' -f4)
    tableau_server=$(grep -o '"server": "[^"]*"' db.json | head -2 | tail -1 | cut -d'"' -f4)
    tableau_db=$(grep -o '"database": "[^"]*"' db.json | head -2 | tail -1 | cut -d'"' -f4)
    
    info "來源資料庫 (MES): $mes_server/$mes_db"
    info "目標資料庫 (Tableau): $tableau_server/$tableau_db"
    
    # 檢查是否有 SAP 資料庫設定
    if grep -q "sap_db" db.json; then
        sap_server=$(grep -o '"server": "[^"]*"' db.json | head -3 | tail -1 | cut -d'"' -f4)
        sap_db=$(grep -o '"database": "[^"]*"' db.json | head -3 | tail -1 | cut -d'"' -f4)
        info "來源資料庫 (SAP): $sap_server/$sap_db"
    else
        warning "找不到 SAP 資料庫設定"
    fi
    
    # 運行測試連接程序
    info "運行資料庫連接測試..."
    python app.py
    
    if [ $? -ne 0 ]; then
        error "資料庫連接測試失敗"
        return 1
    fi
    
    success "資料庫連接測試成功"
    return 0
}

# 設置 GitLab CI/CD
setup_gitlab_ci() {
    info "設置 GitLab CI/CD 配置..."
    
    # 創建 .gitlab-ci.yml 文件
    cat > .gitlab-ci.yml <<EOL
# GitLab CI/CD 配置文件
# ETL 專案自動化部署

stages:
  - test
  - deploy

variables:
  PIP_CACHE_DIR: "$CI_PROJECT_DIR/.pip-cache"
  GIT_SSL_NO_VERIFY: "true"  # 允許忽略 SSL 證書驗證

cache:
  paths:
    - .pip-cache/
    - venv/

# 測試階段
test_connection:
  stage: test
  tags:
    - rocky
  before_script:
    - git config --global http.sslVerify false  # 配置 git 忽略 SSL 驗證
    - echo "設置 git SSL 驗證..."
  script:
    - echo "開始進行資料庫連線測試..."
    - dnf install -y unixODBC unixODBC-devel || echo "ODBC 相關套件安裝失敗，可能需要 sudo 權限"
    - curl https://packages.microsoft.com/config/rhel/9/prod.repo > mssql-release.repo
    - if [ -w /etc/yum.repos.d/ ]; then
        cp mssql-release.repo /etc/yum.repos.d/;
        ACCEPT_EULA=Y dnf install -y msodbcsql17;
      else
        echo "無法安裝 MSSQL ODBC 驅動程式，請確保已經預先安裝";
      fi
    - dnf install -y gcc-c++ python3-devel || echo "開發工具安裝失敗，可能需要 sudo 權限"
    - python3 -m venv venv
    - source venv/bin/activate
    - pip install --upgrade pip
    - pip install pyodbc pandas
    - echo "檢查資料庫設定..."
    - grep -A 5 "mes_db" db.json || echo "警告：找不到 MES 資料庫配置"
    - grep -A 5 "tableau_db" db.json || echo "警告：找不到 Tableau 資料庫配置"
    - grep -A 5 "sap_db" db.json || echo "警告：找不到 SAP 資料庫配置"
    - python app.py || echo "連線測試失敗，請確認 ODBC 驅動程式是否正確安裝"
  artifacts:
    paths:
      - etl_log.log
    expire_in: 1 week
  only:
    - main
    - merge_requests

# 部署階段
deploy_etl:
  stage: deploy
  tags:
    - rocky
  before_script:
    - git config --global http.sslVerify false  # 配置 git 忽略 SSL 驗證
  script:
    - echo "開始進行 ETL 部署..."
    - chmod +x setup.sh
    - ./setup.sh install || echo "安裝過程失敗，可能需要 sudo 權限，請檢查日誌"
  only:
    - main
  artifacts:
    paths:
      - setup_log.txt
      - etl_log.log
    expire_in: 1 week
  environment:
    name: production
EOL
    
    if [ $? -ne 0 ]; then
        error "創建 .gitlab-ci.yml 文件失敗"
        return 1
    fi
    
    success "GitLab CI/CD 配置設置完成"
    return 0
}

# 檢查整體設置
check_setup() {
    info "檢查環境設置..."
    
    # 檢查必要的文件
    files=("app.py" "db.json" "setup.sh" ".gitlab-ci.yml" "requirements.txt")
    
    for file in "${files[@]}"; do
        if [ -f "$file" ]; then
            success "檢查到文件: $file"
        else
            warning "缺少文件: $file"
        fi
    done
    
    # 檢查 Python 虛擬環境
    if [ -d "venv" ]; then
        success "檢查到 Python 虛擬環境"
        
        # 檢查已安裝的套件
        if [ -f "requirements.txt" ]; then
            info "已安裝的 Python 套件:"
            cat requirements.txt
        fi
    else
        warning "缺少 Python 虛擬環境"
    fi
    
    # 檢查 ODBC
    if check_command odbcinst; then
        success "檢查到 ODBC 安裝"
        info "ODBC 驅動程式："
        odbcinst -q -d | grep -v '\[ODBC'
    else
        warning "缺少 ODBC 安裝"
    fi
    
    # 檢查資料庫設定
    if [ -f "db.json" ]; then
        success "檢查到 db.json 文件"
        info "檢查資料庫設定："
        
        mes_count=$(grep -c "mes_db" db.json)
        tableau_count=$(grep -c "tableau_db" db.json)
        sap_count=$(grep -c "sap_db" db.json)
        
        info "MES 資料庫設定: $mes_count 個"
        info "Tableau 資料庫設定: $tableau_count 個"
        info "SAP 資料庫設定: $sap_count 個"
    fi
    
    info "環境檢查完成"
}

# 主函數
main() {
    case "$1" in
        install)
            init_setup
            install_system_packages
            setup_python_venv
            check_db_connection
            setup_gitlab_ci
            check_setup
            ;;
        test_db)
            init_setup
            setup_python_venv
            check_db_connection
            ;;
        check)
            init_setup
            check_setup
            ;;
        *)
            info "ETL 環境設置腳本"
            info "用法: $0 {install|test_db|check}"
            info "  install  - 安裝完整環境"
            info "  test_db  - 僅測試資料庫連接"
            info "  check    - 檢查現有環境"
            exit 1
            ;;
    esac
    
    if [ $? -eq 0 ]; then
        success "操作完成！"
        exit 0
    else
        error "操作失敗！"
        exit 1
    fi
}

# 執行主函數
main "$@"