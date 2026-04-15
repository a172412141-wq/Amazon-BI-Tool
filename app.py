import streamlit as st
import pandas as pd
import numpy as np
import io
import re
from datetime import datetime
import plotly.express as px
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

# ================= 1. 全局配置 =================
st.set_page_config(page_title="亚马逊全息补货中台", page_icon="📦", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    div[data-testid="stMetricValue"] { font-size: 1.5rem !important; font-weight: 800; color: #1E88E5; }
    .sop-box { background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 5px solid #ffc107; margin-bottom: 20px;}
    </style>
""", unsafe_allow_html=True)

# ================= 2. 增强型清洗引擎 =================
def clean_msku_strict(val):
    if pd.isna(val): return ""
    s = str(val).strip()
    return re.sub(r'[\s\u200b\u200c\u200d\uFEFF\x00-\x1f\x7f]', '', s)

def to_numeric_fast(series):
    s = series.astype(str).str.strip().replace(['-', 'nan', 'NaN', 'None', ''], '0')
    s = s.str.replace(',', '') # 处理千分位
    has_pct = s.str.contains('%', na=False)
    s = s.str.replace(r'[^\d.-]', '', regex=True)
    res = pd.to_numeric(s, errors='coerce').fillna(0.0)
    res[has_pct] = res[has_pct] / 100.0
    return res

def find_col(df, keywords):
    """根据关键字列表寻找列名，返回第一个匹配到的"""
    for kw in keywords:
        for c in df.columns:
            if kw.lower() == str(c).lower().strip(): return c
    for kw in keywords:
        for c in df.columns:
            if kw.lower() in str(c).lower(): return c
    return None

# ================= 3. 核心处理函数 =================

@st.cache_data
def process_inventory(file_bytes, fname):
    df = pd.read_excel(io.BytesIO(file_bytes))
    today = pd.Timestamp.now().normalize()
    # 匹配你表头中的“SKU”和“发货量”
    c_sku = find_col(df, ['SKU', '公司SKU'])
    c_qty = find_col(df, ['发货量', '在途数量'])
    c_date = find_col(df, ['XT-预计到货', '预计到货时间', '接收日期'])
    
    if not c_sku or not c_qty: return None
    
    df['join_key'] = df[c_sku].apply(clean_msku_strict)
    df['qty_clean'] = to_numeric_fast(df[c_qty])
    df['date_clean'] = pd.to_datetime(df[c_date], errors='coerce')
    df['days_diff'] = (df['date_clean'] - today).dt.days
    
    # 这里的逻辑根据到货时间归类
    res = df.groupby('join_key').agg({
        'qty_clean': 'sum'
    }).rename(columns={'qty_clean': '待到合计'}).reset_index()
    return res

@st.cache_data
def process_age(file_bytes, fname):
    df = pd.read_excel(io.BytesIO(file_bytes))
    c_sku = find_col(df, ['SKU'])
    if not c_sku: return None
    
    res = pd.DataFrame({'join_key': df[c_sku].apply(clean_msku_strict)})
    # 匹配你表头中的“数量”后缀
    targets = {
        '可用量': ['可用量'],
        '0~30库龄': ['0~30库龄数量', '0~30库龄'],
        '31~60库龄': ['31~60库龄数量', '31~60库龄'],
        '61~90库龄': ['61~90库龄数量', '61~90库龄'],
        '91~180库龄': ['91~180库龄数量', '91~180库龄'],
        '181以上库龄': ['181以上库龄', '181~270库龄数量', '365以上库龄数量']
    }
    for label, keys in targets.items():
        col = find_col(df, keys)
        res[label] = to_numeric_fast(df[col]) if col else 0
        
    return res.groupby('join_key').sum().reset_index()

# ================= 4. 侧边栏与输入 =================
with st.sidebar:
    st.header("📂 数据上传")
    files = st.file_uploader("将产品表现、库龄、海外仓、白名单同时拖入", accept_multiple_files=True)
    
    st.header("⚙️ 参数设置")
    TARGET_TOTAL = st.number_input("目标覆盖天数 (在途+在库)", value=90)
    SAFE_DAYS = st.number_input("断货预警线", value=15)
    run_btn = st.button("🚀 执行全息分析", type="primary", use_container_width=True)

# ================= 5. 主程序逻辑 =================
if run_btn and files:
    with st.spinner("正在穿透多维数据..."):
        df_master = None
        df_inv = []
        df_age_list = []
        df_wl = None
        
        # 1. 文件预分类识别
        for f in files:
            fname = f.name
            f_bytes = f.read()
            if "产品表现" in fname:
                df_master = pd.read_excel(io.BytesIO(f_bytes))
            elif "库龄" in fname:
                df_age_list.append(process_age(f_bytes, fname))
            elif "海外仓" in fname or "在途" in fname:
                df_inv.append(process_inventory(f_bytes, fname))
            elif "白名单" in fname:
                df_wl = pd.read_excel(io.BytesIO(f_bytes))

        if df_master is None:
            st.error("未找到【产品表现】表，请检查文件名！")
            st.stop()

        # 2. 基础数据清洗
        df_master['MSKU'] = df_master[find_col(df_master, ['MSKU', '商家SKU'])].apply(clean_msku_strict)
        df_master['SKU_JOIN'] = df_master[find_col(df_master, ['SKU', 'FNSKU'])].apply(clean_msku_strict)
        
        # 3. 广告数值列强制映射 (解决你不显示广告数据的问题)
        ad_cols = {
            '销售额': ['销售额'], '订单毛利润': ['订单毛利润'], '广告花费': ['广告花费'],
            'ACOS': ['ACOS'], 'ACoAS': ['ACoAS'], 'CPC': ['CPC'], 'CTR': ['CTR'], '广告CVR': ['广告CVR']
        }
        for std_name, keys in ad_cols.items():
            actual_col = find_col(df_master, keys)
            if actual_col:
                df_master[std_name] = to_numeric_fast(df_master[actual_col])
            else:
                df_master[std_name] = 0.0

        # 4. 关联库龄与在途
        final_df = df_master.copy()
        
        if df_age_list:
            df_age_all = pd.concat(df_age_list).groupby('join_key').sum().reset_index()
            final_df = pd.merge(final_df, df_age_all, left_on='SKU_JOIN', right_on='join_key', how='left')
            
        if df_inv:
            df_inv_all = pd.concat(df_inv).groupby('join_key').sum().reset_index()
            final_df = pd.merge(final_df, df_inv_all, left_on='SKU_JOIN', right_on='join_key', how='left')

        # 5. 计算核心指标
        # 假设产品表现表是7天数据，计算日均销量
        final_df['日均销量'] = final_df['销售额'] / (final_df['销售额'].replace(0,1)) # 简单占位逻辑
        # 修正：如果有销售额但没有销量列，我们按均价倒推或直接定义日均
        qty_col = find_col(df_master, ['销量', '订单数', 'Units Ordered'])
        if qty_col:
            final_df['日均销量'] = to_numeric_fast(df_master[qty_col]) / 7
        else:
            final_df['日均销量'] = final_df['销售额'] / 50 / 7 # 估算

        final_df = final_df.fillna(0)
        final_df['总供给'] = final_df.get('可用量', 0) + final_df.get('待到合计', 0)
        final_df['预计可售天数'] = np.where(final_df['日均销量'] > 0, final_df['总供给'] / final_df['日均销量'], 999)
        final_df['建议补货量'] = np.maximum(0, final_df['日均销量'] * TARGET_TOTAL - final_df['总供给'])

        # 6. 白名单过滤
        if df_wl is not None:
            wl_mskus = df_wl[find_col(df_wl, ['MSKU'])].apply(clean_msku_strict).tolist()
            final_df = final_df[final_df['MSKU'].isin(wl_mskus)]

        # ================= 6. 结果展示 =================
        st.title("📊 穿透分析结果")
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("总广告费", f"${final_df['广告花费'].sum():,.2f}")
        m2.metric("平均ACOS", f"{final_df['ACOS'].mean():.2%}")
        m3.metric("需补货SKU数", len(final_df[final_df['建议补货量'] > 0]))
        m4.metric("断货风险SKU", len(final_df[final_df['预计可售天数'] < SAFE_DAYS]))

        # 可视化
        st.subheader("🚀 断货预警排行 (TOP 10)")
        risk_top = final_df[final_df['日均销量'] > 0].nsmallest(10, '预计可售天数')
        fig = px.bar(risk_top, x='预计可售天数', y='MSKU', orientation='h', 
                     color='预计可售天数', color_continuous_scale='Reds_r')
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("📝 明细看板")
        show_cols = ['店铺', 'MSKU', 'ASIN', '销售额', '广告花费', 'ACOS', '可用量', '待到合计', '预计可售天数', '建议补货量']
        st.dataframe(final_df[show_cols].style.background_gradient(subset=['预计可售天数'], cmap='RdYlGn'), use_container_width=True)

        # 下载
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False, sheet_name='补货建议')
        st.download_button("📥 下载完整分析报告", output.getvalue(), "智能补货.xlsx", type="primary")
