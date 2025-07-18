#!/bin/bash

# 設置 ETL 排程任務
# 執行此腳本以設置 crontab

# 確保腳本有執行權限
chmod +x /home/ETL/etl/etl_scheduler.sh

# 建立臨時 crontab 文件
TEMP_CRONTAB=$(mktemp)

# 導出現有 crontab
crontab -l > $TEMP_CRONTAB 2>/dev/null || echo "# 開始設置 crontab" > $TEMP_CRONTAB

# 檢查是否已經存在相同的排程
if ! grep -q "etl_scheduler.sh" $TEMP_CRONTAB; then
    # 新增排程 (每天 16:00 和 00:00)
    echo "# ETL 排程 - 每天 16:00 和 00:00 執行" >> $TEMP_CRONTAB
    echo "0 16 * * * /home/ETL/etl/etl_scheduler.sh" >> $TEMP_CRONTAB
    echo "0 0 * * * /home/ETL/etl/etl_scheduler.sh" >> $TEMP_CRONTAB
    
    # 應用新的 crontab
    crontab $TEMP_CRONTAB
    echo "ETL 排程已成功設置！"
else
    echo "ETL 排程已存在，無需重新設置。"
fi

# 清理臨時文件
rm $TEMP_CRONTAB

# 顯示當前 crontab
echo "當前 crontab 設置:"
crontab -l
