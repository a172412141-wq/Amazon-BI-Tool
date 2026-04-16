import streamlit as st
import pandas as pd
import numpy as np
import re
import io
from openpyxl.styles import Font, Alignment, PatternFill
from openpyxl.utils import get_column_letter

# ================= 1. 页面与侧边栏配置 =================
st.set_page_config(page_title="亚马逊智能补货系统", page_icon="🚀", layout="wide")

st.sidebar.header("⚙️ 补货计算参数配置")
TARGET_DAYS_TRANSIT = st.sidebar.number_input("目标物流在途天数", min_value=0, value=50, step=1)
TARGET_DAYS_STOCK = st.sidebar.number_input("目标安全在库天数", min_value=0, value=30, step=1)
TARGET_DAYS_TOTAL = TARGET_DAYS_TRANSIT + TARGET_DAYS_STOCK
ALERT_STOCKOUT_DAYS = st.sidebar.number_input("缺货预警天数阈值", min_value=0, value=20, step=1)

# ================= 2. 基础函数 =================
def clean_msku_strict(val):
    if pd.isna(val): return ""
    return re.sub(r'\s+', '', str(val).strip())

def clean_percentage_or_money(x, col_name=""):
    if pd.isna(x): return 0.0
    s = str(x).strip()
    if s == '-': return 0.0
    s_clean = re.sub(r'[^\d.-]', '', s)
    try:
        val = float(s_clean)
        if any(k in col_name.upper() for k in ['率', 'CTR', 'ACOS', 'ACoAS', 'CVR']):
            if '%' in s: val = val / 100.0
            elif val > 1.0 and any(k in col_name.upper() for k in ['CTR', '转化率', 'CVR', 'ACOS']):
                val = val / 100.0
        return val
    except: return 0.0

def clean_columns(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df

def find_col_fuzzy_priority(df, keywords_list):
    for kw in keywords_list:
        for col in df.columns:
            if kw.lower() in col.lower(): return col
    return None

def read_uploaded_file(file):
    if file.name.endswith('.csv'):
        try: return pd.read_csv(file, encoding='utf-8')
        except: 
            file.seek(0)
            return pd.read_csv(file, encoding='gbk')
    return pd.read_excel(file)

# ================= 3. 数据处理 =================
def process_traffic_df(uploaded_files, prefix):
    if not uploaded_files: return None
    dfs = []
    for f in uploaded_files:
        df = read_uploaded_file(f)
        df = clean_columns(df)
        sku_col = find_col_fuzzy_priority(df, ['SKU', '(Child)', '子ASIN'])
        if not sku_col: continue
        df['join_key'] = df[sku_col].apply(clean_msku_strict)
        shop_col = find_col_fuzzy_priority(df, ['店铺', 'Shop', 'Store'])
        df['traffic_shop'] = df[shop_col].astype(str).str.strip() if shop_col else 'Unknown'
        
        indicators = [("会话", "会话数"), ("页面浏览", "页面浏览量"), ("订单商品", "订单商品总数"), ("销售额", "销售额")]
        rename_dict = {}
        for keyword, suffix in indicators:
            for col in df.columns:
                if keyword in col and "B2B" not in col:
                    df[col] = df[col].apply(lambda x: clean_percentage_or_money(x, col))
                    rename_dict[col] = f"{prefix}{suffix}"
                    break
        dfs.append(df[['join_key', 'traffic_shop'] + list(rename_dict.keys())].rename(columns=rename_dict))
    return pd.concat(dfs).groupby(['join_key', 'traffic_shop']).sum().reset_index() if dfs else None

# ================= 4. 主程序 =================
st.title("🚀 亚马逊智能补货系统 (优化版)")

col1, col2 = st.columns(2)
with col1:
    files_prod = st.file_uploader("📂 产品表现表 (主表/重要表)", accept_multiple_files=True)
    files_7d = st.file_uploader("📂 7天流量表", accept_multiple_files=True)
with col2:
    files_inv = st.file_uploader("📦 库存表", accept_multiple_files=True)
    files_age = st.file_uploader("👴 库龄表", accept_multiple_files=True)

if st.button("✨ 生成并格式化报表", type="primary"):
    if not files_prod:
        st.error("请至少上传产品表现表！")
    else:
        with st.spinner('正在处理并应用格式优化...'):
            # A. 基础合并逻辑
            all_data = []
            for f in files_prod:
                d = clean_columns(read_uploaded_file(f))
                col_msku = find_col_fuzzy_priority(d, ['MSKU', '商家SKU'])
                if col_msku:
                    d['MSKU'] = d[col_msku].apply(clean_msku_strict)
                    d['SKU_KEY'] = d['MSKU']
                    all_data.append(d)
            
            df_master = pd.concat(all_data, ignore_index=True)
            df_7 = process_traffic_df(files_7d, "7天")
            
            # 合并逻辑 (简化展示)
            merged = pd.merge(df_master, df_7, left_on='MSKU', right_on='join_key', how='left').drop(columns=['join_key'], errors='ignore')
            
            # 补货计算 (示例)
            merged['7天日均'] = merged.get('7天订单商品总数', 0) / 7
            merged['建议补货量'] = (merged['7天日均'] * TARGET_DAYS_TOTAL).round(0)
            
            # 数值预处理：所有浮点数保留两位小数
            merged = merged.round(2)

            # B. 格式调整与导出
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                merged.to_excel(writer, index=False, sheet_name='补货数据')
                ws = writer.sheets['补货数据']
                
                # 预留求和行位置
                ws.insert_rows(1, amount=2) 
                
                # 定义样式
                # 1. 全文水平/垂直居中
                # 2. 第三行自动换行
                base_alignment = Alignment(horizontal='center', vertical='center')
                header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
                
                # 设置第 3 行行高为 80
                ws.row_dimensions[3].height = 80
                
                # 遍历所有单元格应用格式
                for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=ws.max_row), 1):
                    for cell in row:
                        # 全文水平垂直居中
                        if row_idx == 3:
                            cell.alignment = header_alignment # 第三行额外开启自动换行
                            cell.font = Font(bold=True)
                        else:
                            cell.alignment = base_alignment
                        
                        # 格式控制：所有数值保留小数点后两位
                        if isinstance(cell.value, (int, float)):
                            cell.number_format = '0.00'

                # 冻结窗口
                ws.freeze_panes = 'A4'

            output.seek(0)
            st.success("🎉 报表格式优化完成！")
            st.download_button(
                label="📥 下载优化后的补货表",
                data=output,
                file_name="亚马逊补货表_优化版.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
