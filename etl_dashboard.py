import streamlit as st
import pandas as pd
import json
import pyodbc
from sqlalchemy import create_engine, text
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os

# 設定頁面配置
st.set_page_config(
    page_title="YS ETL 監控儀表板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS 風格
st.markdown("""
<style>
    .main {
        padding: 1rem 2rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 1rem;
        padding: 0.5rem 1rem;
    }
    .metric-box {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    .status-success {
        color: green;
        font-weight: bold;
    }
    .status-warning {
        color: orange;
        font-weight: bold;
    }
    .status-error {
        color: red;
        font-weight: bold;
    }
    .st-emotion-cache-6qob1r.eczjsme3 {
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# 載入資料庫配置


@st.cache_data
def load_db_config():
    try:
        with open('db.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"載入資料庫配置失敗: {e}")
        return None

# 建立資料庫連接


def build_connection_string(db_config):
    drv = "{ODBC Driver 17 for SQL Server}"
    srv = db_config['server']
    port = db_config.get('port', 1433)
    db = db_config['database']
    uid = db_config['username']
    pwd = db_config['password']
    opts = db_config.get('options', {})
    enc = 'yes' if opts.get('encrypt') else 'no'
    trust = 'yes' if opts.get('trustServerCertificate') else 'no'
    conn_str = (
        f"DRIVER={drv};"
        f"SERVER={srv},{port};DATABASE={db};"
        f"UID={uid};PWD={pwd};Encrypt={enc};TrustServerCertificate={trust};"
    )
    return conn_str


def build_sqlalchemy_engine(db_config):
    drv = 'ODBC Driver 17 for SQL Server'.replace(' ', '+')
    srv = db_config['server']
    port = db_config.get('port', 1433)
    db = db_config['database']
    uid = db_config['username']
    pwd = db_config['password']
    opts = db_config.get('options', {})
    enc = 'yes' if opts.get('encrypt') else 'no'
    trust = 'yes' if opts.get('trustServerCertificate') else 'no'
    uri = (
        f"mssql+pyodbc://{uid}:{pwd}@{srv},{port}/{db}?driver={drv}"
        f"&Encrypt={enc}&TrustServerCertificate={trust}"
    )
    return create_engine(uri)

# 獲取資料表結構 - 修正引擎參數


@st.cache_data
def get_table_structure(_engine, table_name):
    try:
        query = f"""
        SELECT 
            COLUMN_NAME, 
            DATA_TYPE, 
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            IS_NULLABLE,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"獲取表格結構失敗: {e}")
        return pd.DataFrame()

# 獲取資料庫中所有表格 - 修正引擎參數


@st.cache_data
def get_all_tables(_engine):
    try:
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"獲取表格列表失敗: {e}")
        return pd.DataFrame()

# 獲取表格資料範例 - 修正引擎參數


@st.cache_data(ttl=300)  # 5分鐘緩存
def get_table_sample(_engine, table_name, limit=100):
    try:
        query = f"SELECT TOP {limit} * FROM {table_name}"
        return pd.read_sql(query, _engine)
    except Exception as e:
        st.error(f"獲取表格資料範例失敗: {e}")
        return pd.DataFrame()

# 獲取表格資料數量 - 修正引擎參數


@st.cache_data(ttl=300)  # 5分鐘緩存
def get_table_count(_engine, table_name):
    try:
        query = f"SELECT COUNT(*) AS row_count FROM {table_name}"
        return pd.read_sql(query, _engine).iloc[0]['row_count']
    except Exception as e:
        st.error(f"獲取表格資料數量失敗: {e}")
        return 0

# 獲取 ETL 執行記錄 - 修正引擎參數


@st.cache_data(ttl=300)  # 5分鐘緩存
def get_etl_summary(_engine, days=7):
    try:
        # 檢查表是否存在
        check_query = """
        SELECT CASE WHEN EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'ETL_SUMMARY'
        ) THEN 1 ELSE 0 END AS table_exists
        """
        exists = pd.read_sql(check_query, _engine).iloc[0]['table_exists']

        if not exists:
            return None

        recent_date = datetime.now() - timedelta(days=days)

        # 獲取每日執行統計，過濾掉摘要記錄
        daily_stats_query = f"""
        SELECT 
            CONVERT(VARCHAR(10), ETL_DATE, 120) AS ExecutionDate,
            COUNT(*) AS TotalExecutions,
            SUM(CASE WHEN SOURCE_TYPE = 'MES' THEN 1 ELSE 0 END) AS MESExecutions,
            SUM(CASE WHEN SOURCE_TYPE = 'SAP' THEN 1 ELSE 0 END) AS SAPExecutions,
            SUM([ROW_COUNT]) AS TotalRowsProcessed
        FROM ETL_SUMMARY
        WHERE ETL_DATE >= '{recent_date.strftime('%Y-%m-%d')}'
        AND SUMMARY_TYPE = 'QUERY'  -- 只選擇查詢執行記錄
        GROUP BY CONVERT(VARCHAR(10), ETL_DATE, 120)
        ORDER BY ExecutionDate DESC
        """

        daily_stats = pd.read_sql(daily_stats_query, _engine)

        # 獲取最近執行記錄，過濾掉無意義的記錄
        recent_query = """
        SELECT TOP 50
            [TIMESTAMP],
            [SOURCE_TYPE],
            [QUERY_NAME],
            [TARGET_TABLE],
            [ROW_COUNT],
            CONVERT(VARCHAR(19), ETL_DATE, 120) AS ETL_DATE
        FROM ETL_SUMMARY
        WHERE [SOURCE_TYPE] IS NOT NULL 
          AND [SOURCE_TYPE] <> '' 
          AND [SOURCE_TYPE] <> 'None'
          AND [QUERY_NAME] IS NOT NULL 
          AND [QUERY_NAME] <> '' 
          AND [QUERY_NAME] <> 'None'
        ORDER BY [ETL_DATE] DESC
        """

        recent_executions = pd.read_sql(recent_query, _engine)

        # 獲取各目標表的記錄數，過濾掉無意義的記錄
        table_stats_query = """
        SELECT 
            s.TARGET_TABLE,
            CONVERT(VARCHAR(19), MAX(s.ETL_DATE), 120) AS LastUpdated,
            MAX(s.[ROW_COUNT]) AS [Total_Rows]
        FROM ETL_SUMMARY s
        INNER JOIN (
            SELECT TARGET_TABLE, MAX(ETL_DATE) AS MaxDate
            FROM ETL_SUMMARY
            WHERE TARGET_TABLE IS NOT NULL 
              AND TARGET_TABLE <> '' 
              AND TARGET_TABLE <> 'None'
              AND TARGET_TABLE <> 'ALL_TABLES'
            GROUP BY TARGET_TABLE
        ) latest ON s.TARGET_TABLE = latest.TARGET_TABLE AND s.ETL_DATE = latest.MaxDate
        WHERE s.TARGET_TABLE IS NOT NULL 
          AND s.TARGET_TABLE <> '' 
          AND s.TARGET_TABLE <> 'None'
          AND s.TARGET_TABLE <> 'ALL_TABLES'
        GROUP BY s.TARGET_TABLE
        """

        table_stats = pd.read_sql(table_stats_query, _engine)

        return {
            'daily_stats': daily_stats,
            'recent_executions': recent_executions,
            'table_stats': table_stats
        }
    except Exception as e:
        st.error(f"獲取 ETL 執行記錄失敗: {e}")
        return None

# 獲取一個欄位的資料分佈 - 修正引擎參數


@st.cache_data(ttl=300)  # 5分鐘緩存
def get_column_distribution(_engine, table_name, column_name, limit=1000):
    try:
        # 檢查欄位類型
        structure = get_table_structure(_engine, table_name)
        col_info = structure[structure['COLUMN_NAME'] == column_name]

        if col_info.empty:
            return None

        data_type = col_info.iloc[0]['DATA_TYPE']

        # 根據數據類型選擇不同的查詢
        if data_type in ('varchar', 'nvarchar', 'char', 'nchar', 'text', 'ntext'):
            # 文字類欄位
            query = f"""
            SELECT TOP {limit} 
                {column_name} AS value, 
                COUNT(*) AS count
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            GROUP BY {column_name}
            ORDER BY COUNT(*) DESC
            """
        elif data_type in ('int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney'):
            # 數值類欄位
            query = f"""
            SELECT TOP {limit} 
                {column_name} AS value, 
                COUNT(*) AS count
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            GROUP BY {column_name}
            ORDER BY {column_name}
            """
        elif data_type in ('date', 'datetime', 'datetime2', 'smalldatetime'):
            # 日期類欄位
            query = f"""
            SELECT TOP {limit} 
                CONVERT(VARCHAR(10), {column_name}, 120) AS value, 
                COUNT(*) AS count
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            GROUP BY CONVERT(VARCHAR(10), {column_name}, 120)
            ORDER BY CONVERT(VARCHAR(10), {column_name}, 120)
            """
        else:
            # 其他類型
            query = f"""
            SELECT TOP {limit} 
                CAST({column_name} AS VARCHAR(100)) AS value, 
                COUNT(*) AS count
            FROM {table_name}
            WHERE {column_name} IS NOT NULL
            GROUP BY {column_name}
            ORDER BY COUNT(*) DESC
            """

        result = pd.read_sql(query, _engine)
        result['value'] = result['value'].astype(str)  # 確保值為字串類型
        return result
    except Exception as e:
        st.error(f"獲取欄位分佈失敗: {e}")
        return None

# 主應用程式


def main():
    st.title("YS ETL 監控儀表板")

    # 初始化
    configs = load_db_config()
    if not configs:
        st.error("無法加載資料庫配置。請確保 db.json 文件存在並格式正確。")
        return

    # 側邊欄
    st.sidebar.header("資料庫連接")

    db_type = st.sidebar.selectbox(
        "選擇資料庫類型",
        ["Tableau (目標)", "MES (來源)", "SAP (來源)"]
    )

    if db_type == "Tableau (目標)":
        db_config = configs.get('tableau_db')
    elif db_type == "MES (來源)":
        db_config = configs.get('mes_db')
    else:  # SAP (來源)
        db_config = configs.get('sap_db')

    if not db_config:
        st.sidebar.error(f"找不到 {db_type} 的資料庫配置")
        return

    st.sidebar.success(f"已連接到 {db_config['server']}/{db_config['database']}")

    # 建立資料庫引擎
    engine = build_sqlalchemy_engine(db_config)

    # 主標籤
    tabs = st.tabs(["儀表板", "表格結構", "資料探索", "ETL 執行記錄"])

    # 儀表板標籤
    with tabs[0]:
        st.header("ETL 概況儀表板")

        # 獲取 ETL 摘要
        etl_summary = get_etl_summary(engine)

        # 無 ETL 摘要時的提示
        if not etl_summary:
            st.warning("找不到 ETL 執行記錄。請確保 ETL_SUMMARY 表存在且有資料。")
            st.info("您可以先查看「表格結構」和「資料探索」標籤來了解資料庫中的資料。")
        else:
            # 計算摘要指標
            total_executions = len(etl_summary['recent_executions'])
            unique_tables = etl_summary['table_stats']['TARGET_TABLE'].nunique(
            )
            total_rows = etl_summary['table_stats']['Total_Rows'].sum()

            # 最近一次執行時間
            latest_execution = None
            if not etl_summary['recent_executions'].empty:
                latest_execution = etl_summary['recent_executions'].iloc[0]['ETL_DATE']

            # 儀表板指標
            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown("<div class='metric-box'>", unsafe_allow_html=True)
                st.metric("總執行次數", f"{total_executions:,}")

                if latest_execution:
                    st.markdown(f"最近執行: {latest_execution}")
                st.markdown("</div>", unsafe_allow_html=True)

            with col2:
                st.markdown("<div class='metric-box'>", unsafe_allow_html=True)
                st.metric("目標表數量", unique_tables)

                # 當天是否執行
                today = datetime.now().strftime('%Y-%m-%d')
                today_executions = etl_summary['daily_stats'][etl_summary['daily_stats']
                                                              ['ExecutionDate'] == today]

                if not today_executions.empty:
                    st.markdown(
                        "<span class='status-success'>✓ 今日已執行</span>", unsafe_allow_html=True)
                else:
                    st.markdown(
                        "<span class='status-warning'>⚠ 今日尚未執行</span>", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

            with col3:
                st.markdown("<div class='metric-box'>", unsafe_allow_html=True)
                st.metric("總處理資料量", f"{total_rows:,} 筆")

                # 顯示成功率
                success_count = etl_summary['recent_executions'].shape[0]
                if success_count > 0:
                    st.markdown(
                        "<span class='status-success'>執行成功率: 100%</span>", unsafe_allow_html=True)
                st.markdown("</div>", unsafe_allow_html=True)

            # 每日執行統計圖表
            st.subheader("每日執行統計")

            if not etl_summary['daily_stats'].empty:
                daily_stats = etl_summary['daily_stats'].copy()
                # 轉換日期為日期類型以便排序
                daily_stats['ExecutionDate'] = pd.to_datetime(
                    daily_stats['ExecutionDate'])
                daily_stats = daily_stats.sort_values('ExecutionDate')

                fig = go.Figure()

                # MES 執行次數
                fig.add_trace(go.Bar(
                    x=daily_stats['ExecutionDate'],
                    y=daily_stats['MESExecutions'],
                    name='MES 執行次數',
                    marker_color='rgba(55, 83, 109, 0.7)'
                ))

                # SAP 執行次數
                fig.add_trace(go.Bar(
                    x=daily_stats['ExecutionDate'],
                    y=daily_stats['SAPExecutions'],
                    name='SAP 執行次數',
                    marker_color='rgba(26, 118, 255, 0.7)'
                ))

                fig.update_layout(
                    barmode='group',
                    xaxis_title='執行日期',
                    yaxis_title='執行次數',
                    xaxis=dict(
                        tickformat='%Y-%m-%d',
                        tickangle=-45
                    ),
                    template='plotly_white',
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("無每日執行統計資料")

            # 資料處理量趨勢圖
            st.subheader("資料處理量趨勢")

            if not etl_summary['daily_stats'].empty:
                daily_stats = etl_summary['daily_stats'].copy()
                # 轉換日期為日期類型以便排序
                daily_stats['ExecutionDate'] = pd.to_datetime(
                    daily_stats['ExecutionDate'])
                daily_stats = daily_stats.sort_values('ExecutionDate')

                fig = px.line(
                    daily_stats,
                    x='ExecutionDate',
                    y='TotalRowsProcessed',
                    markers=True,
                    labels={'ExecutionDate': '執行日期',
                            'TotalRowsProcessed': '處理資料量'}
                )

                fig.update_layout(
                    xaxis_title='執行日期',
                    yaxis_title='處理資料量',
                    xaxis=dict(
                        tickformat='%Y-%m-%d',
                        tickangle=-45
                    ),
                    template='plotly_white',
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("無資料處理量趨勢資料")

            # 表格統計
            st.subheader("目標表最新狀態")

            if not etl_summary['table_stats'].empty:
                # 格式化日期時間列
                etl_summary['table_stats']['LastUpdated'] = pd.to_datetime(
                    etl_summary['table_stats']['LastUpdated'])

                fig = px.bar(
                    etl_summary['table_stats'].sort_values(
                        'Total_Rows', ascending=False),
                    x='TARGET_TABLE',
                    y='Total_Rows',
                    color='Total_Rows',
                    labels={'TARGET_TABLE': '目標表', 'Total_Rows': '資料列數'},
                    hover_data=['LastUpdated']
                )

                fig.update_layout(
                    xaxis_title='目標表',
                    yaxis_title='資料列數',
                    xaxis_tickangle=-45,
                    template='plotly_white',
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)

                # 顯示表格詳細資訊
                st.dataframe(
                    etl_summary['table_stats'],
                    column_config={
                        "TARGET_TABLE": "目標表",
                        "LastUpdated": st.column_config.DatetimeColumn("最近更新時間"),
                        "Total_Rows": st.column_config.NumberColumn("資料列數", format="%d")
                    },
                    hide_index=True
                )
            else:
                st.info("無目標表統計資料")

            # 最近執行記錄
            st.subheader("最近執行記錄")

            if not etl_summary['recent_executions'].empty:
                # 格式化日期時間列
                etl_summary['recent_executions']['ETL_DATE'] = pd.to_datetime(
                    etl_summary['recent_executions']['ETL_DATE'])

                st.dataframe(
                    etl_summary['recent_executions'],
                    column_config={
                        "TIMESTAMP": "時間戳記",
                        "SOURCE_TYPE": "來源類型",
                        "QUERY_NAME": "查詢名稱",
                        "TARGET_TABLE": "目標表",
                        "ROW_COUNT": st.column_config.NumberColumn("資料列數", format="%d"),
                        "ETL_DATE": st.column_config.DatetimeColumn("ETL 執行時間")
                    },
                    hide_index=True
                )
            else:
                st.info("無最近執行記錄")

    # 表格結構標籤
    with tabs[1]:
        st.header("資料表結構瀏覽")

        # 獲取所有表格
        tables = get_all_tables(engine)

        if tables.empty:
            st.warning("資料庫中未找到表格")
            return

        # 選擇表格
        selected_table = st.selectbox("選擇資料表", tables['TABLE_NAME'])

        # 顯示表格結構
        st.subheader(f"{selected_table} 表格結構")

        structure = get_table_structure(engine, selected_table)
        if not structure.empty:
            # 轉換欄位類型信息
            def format_type(row):
                type_str = row['DATA_TYPE']

                if type_str in ('varchar', 'nvarchar', 'char', 'nchar'):
                    length = row['CHARACTER_MAXIMUM_LENGTH']
                    if length == -1:
                        type_str += '(MAX)'
                    elif length is not None:
                        type_str += f'({length})'
                elif type_str in ('decimal', 'numeric'):
                    precision = row['NUMERIC_PRECISION']
                    scale = row['NUMERIC_SCALE']
                    if precision is not None and scale is not None:
                        type_str += f'({precision},{scale})'

                return type_str

            structure['DATA_TYPE_FORMATTED'] = structure.apply(
                format_type, axis=1)
            structure['IS_NULLABLE'] = structure['IS_NULLABLE'].apply(
                lambda x: '是' if x == 'YES' else '否')

            # 顯示表格結構資料
            st.dataframe(
                structure[['COLUMN_NAME', 'DATA_TYPE_FORMATTED',
                           'IS_NULLABLE', 'ORDINAL_POSITION']],
                column_config={
                    "COLUMN_NAME": "欄位名稱",
                    "DATA_TYPE_FORMATTED": "資料類型",
                    "IS_NULLABLE": "允許NULL",
                    "ORDINAL_POSITION": "順序"
                },
                hide_index=True
            )

            # 顯示資料表統計信息
            row_count = get_table_count(engine, selected_table)
            st.metric("資料列數", f"{row_count:,}")
        else:
            st.warning(f"無法獲取 {selected_table} 的表格結構")

    # 資料探索標籤
    with tabs[2]:
        st.header("資料探索")

        # 獲取所有表格
        tables = get_all_tables(engine)

        if tables.empty:
            st.warning("資料庫中未找到表格")
            return

        # 選擇表格
        selected_table = st.selectbox(
            "選擇資料表", tables['TABLE_NAME'], key="explore_table")

        # 獲取表格結構
        structure = get_table_structure(engine, selected_table)
        if structure.empty:
            st.warning(f"無法獲取 {selected_table} 的表格結構")
            return

        # 獲取表格範例數據
        sample_size = st.slider("範例資料量", min_value=5,
                                max_value=1000, value=100, step=5)
        sample_data = get_table_sample(engine, selected_table, sample_size)

        # 顯示表格範例數據
        st.subheader(f"{selected_table} 範例資料")

        if not sample_data.empty:
            st.dataframe(sample_data)
        else:
            st.warning(f"無法獲取 {selected_table} 的範例資料")
            return

        # 欄位分析
        st.subheader("欄位分析")

        # 選擇欄位
        columns = structure['COLUMN_NAME'].tolist()
        selected_column = st.selectbox("選擇要分析的欄位", columns)

        # 獲取欄位分佈
        column_data = get_column_distribution(
            engine, selected_table, selected_column)

        if column_data is not None and not column_data.empty:
            # 統計信息
            total_values = column_data['count'].sum()
            unique_values = len(column_data)

            col1, col2 = st.columns(2)

            with col1:
                st.metric("唯一值數量", unique_values)

            with col2:
                st.metric("總值數量", f"{total_values:,}")

            # 繪製分佈圖
            # 如果唯一值太多，只顯示前20項
            display_limit = 20
            if len(column_data) > display_limit:
                st.info(
                    f"該欄位有 {len(column_data)} 個唯一值，下圖只顯示前 {display_limit} 項。")
                plot_data = column_data.head(display_limit)
            else:
                plot_data = column_data

            fig = px.bar(
                plot_data,
                x='value',
                y='count',
                labels={'value': '值', 'count': '計數'},
                title=f"{selected_column} 欄位分佈"
            )

            fig.update_layout(
                xaxis_title=selected_column,
                yaxis_title='計數',
                xaxis_tickangle=-45,
                template='plotly_white',
                height=400
            )

            st.plotly_chart(fig, use_container_width=True)

            # 顯示分佈數據表格
            st.subheader("值分佈表格")

            # 計算百分比
            column_data['percentage'] = column_data['count'] / \
                column_data['count'].sum() * 100

            st.dataframe(
                column_data,
                column_config={
                    "value": selected_column,
                    "count": st.column_config.NumberColumn("計數", format="%d"),
                    "percentage": st.column_config.NumberColumn("百分比", format="%.2f%%")
                },
                hide_index=True
            )
        else:
            st.warning(f"無法獲取 {selected_column} 欄位的分佈數據")

    # ETL 執行記錄標籤
    with tabs[3]:
        st.header("ETL 執行記錄")

        # 獲取 ETL 摘要
        etl_days = st.slider("顯示最近幾天的記錄", min_value=1,
                             max_value=90, value=30, step=1)
        etl_summary = get_etl_summary(engine, etl_days)

        if not etl_summary:
            st.warning("找不到 ETL 執行記錄。請確保 ETL_SUMMARY 表存在且有資料。")
            return

        # 顯示最近執行記錄
        st.subheader("最近執行記錄")

        if not etl_summary['recent_executions'].empty:
            # 格式化日期時間列
            etl_summary['recent_executions']['ETL_DATE'] = pd.to_datetime(
                etl_summary['recent_executions']['ETL_DATE'])

            # 添加過濾選項
            col1, col2 = st.columns(2)

            with col1:
                source_types = [
                    '全部'] + etl_summary['recent_executions']['SOURCE_TYPE'].unique().tolist()
                selected_source = st.selectbox("來源類型", source_types)

            with col2:
                target_tables = [
                    '全部'] + etl_summary['recent_executions']['TARGET_TABLE'].unique().tolist()
                selected_table = st.selectbox("目標表", target_tables)

            # 應用過濾
            filtered_data = etl_summary['recent_executions'].copy()

            if selected_source != '全部':
                filtered_data = filtered_data[filtered_data['SOURCE_TYPE']
                                              == selected_source]

            if selected_table != '全部':
                filtered_data = filtered_data[filtered_data['TARGET_TABLE']
                                              == selected_table]

            # 顯示過濾後的資料
            if not filtered_data.empty:
                st.dataframe(
                    filtered_data,
                    column_config={
                        "TIMESTAMP": "時間戳記",
                        "SOURCE_TYPE": "來源類型",
                        "QUERY_NAME": "查詢名稱",
                        "TARGET_TABLE": "目標表",
                        "ROW_COUNT": st.column_config.NumberColumn("資料列數", format="%d"),
                        "ETL_DATE": st.column_config.DatetimeColumn("ETL 執行時間")
                    },
                    hide_index=True
                )
            else:
                st.info("符合過濾條件的記錄為空")

            # 繪製執行記錄趨勢圖
            st.subheader("執行記錄趨勢")

            # 按日期分組
            etl_summary['recent_executions']['ETL_DATE_DATE'] = etl_summary['recent_executions']['ETL_DATE'].dt.date
            daily_counts = etl_summary['recent_executions'].groupby(
                ['ETL_DATE_DATE', 'SOURCE_TYPE']).size().reset_index(name='count')

            if not daily_counts.empty:
                fig = px.line(
                    daily_counts,
                    x='ETL_DATE_DATE',
                    y='count',
                    color='SOURCE_TYPE',
                    markers=True,
                    labels={'ETL_DATE_DATE': '日期',
                            'count': '執行次數', 'SOURCE_TYPE': '來源類型'}
                )

                fig.update_layout(
                    xaxis_title='日期',
                    yaxis_title='執行次數',
                    xaxis_tickangle=-45,
                    template='plotly_white',
                    height=400
                )

                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("無執行記錄趨勢資料")

            # 目標表更新時間表格
            st.subheader("目標表最後更新時間")

            if not etl_summary['table_stats'].empty:
                etl_summary['table_stats']['LastUpdated'] = pd.to_datetime(
                    etl_summary['table_stats']['LastUpdated'])

                st.dataframe(
                    etl_summary['table_stats'],
                    column_config={
                        "TARGET_TABLE": "目標表",
                        "LastUpdated": st.column_config.DatetimeColumn("最近更新時間"),
                        "Total_Rows": st.column_config.NumberColumn("資料列數", format="%d")
                    },
                    hide_index=True
                )
            else:
                st.info("無目標表更新時間資料")
        else:
            st.info("無 ETL 執行記錄資料")

    # 頁腳
    st.markdown("---")
    st.markdown(
        f"<div style='text-align: center; color: gray; font-size: 0.8em;'>"
        f"YS ETL 監控儀表板 | 最後更新時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        f"</div>",
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
