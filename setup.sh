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
        
    elif check_command yum; then
        info "使用 yum 包管理器..."
        
        sudo yum update -y
        if [ $? -ne 0 ]; then
            error "無法更新套件清單"
            return 1
        fi
        
        info "安裝必要的系統套件..."
        sudo yum install -y \
            python3 \
            python3-pip \
            unixODBC \
            unixODBC-devel \
            curl
            
        if [ $? -ne 0 ]; then
            error "安裝基本套件失敗"
            return 1
        fi
        
        # 安裝 ODBC Driver for SQL Server
        info "安裝 Microsoft ODBC Driver for SQL Server..."
        
        # 下載 Microsoft 儲存庫 RPM
        sudo curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/mssql-release.repo
        
        # 安裝 ODBC driver
        sudo ACCEPT_EULA=Y yum install -y msodbcsql17
        
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
    
    # 激活虛擬環境
    info "激活虛擬環境..."
    source venv/bin/activate
    
    if [ $? -ne 0 ]; then
        error "無法激活虛擬環境"
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
    source_server=$(grep -o '"server": "[^"]*"' db.json | head -1 | cut -d'"' -f4)
    source_db=$(grep -o '"database": "[^"]*"' db.json | head -1 | cut -d'"' -f4)
    target_server=$(grep -o '"server": "[^"]*"' db.json | head -2 | tail -1 | cut -d'"' -f4)
    target_db=$(grep -o '"database": "[^"]*"' db.json | head -2 | tail -1 | cut -d'"' -f4)
    
    info "來源資料庫: $source_server/$source_db"
    info "目標資料庫: $target_server/$target_db"
    
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
# GitLab CI/CD 配置

stages:
  - test
  - deploy

variables:
  PIP_CACHE_DIR: "$CI_PROJECT_DIR/.pip-cache"

cache:
  paths:
    - .pip-cache/
    - venv/

before_script:
  - python3 -V
  - pip3 -V

test_connection:
  stage: test
  script:
    - bash setup.sh test_db
  only:
    - main
    - merge_requests

deploy_etl:
  stage: deploy
  script:
    - bash setup.sh install
  only:
    - main
  artifacts:
    paths:
      - setup_log.txt
      - etl_log.log
    expire_in: 1 week
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