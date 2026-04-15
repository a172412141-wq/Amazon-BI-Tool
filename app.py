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
st.set_page_config(page_title="智能补货与数据中台", page_icon="📦", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    h1, h2, h3, h4, h5 { margin-bottom: 0.1rem !important; padding-bottom: 0.1rem !important; margin-top: 0.1rem !important; }
    div[data-testid="metric-container"] { padding: 0px; margin: 0px; }
    div[data-testid="stMetricValue"] { font-size: 1.5rem !important; font-weight: 800; color: #1E88E5; }
    div[data-testid="stMetricLabel"] { font-size: 0.9rem !important; font-weight: bold; margin-bottom: -5px; }
    .sop-box { background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 5px solid #ffc107; font-size: 0.9em; color: #333;}
    </style>
""", unsafe_allow_html=True)
st.title("🚀 亚马逊智能补货与全息分析中台")

# ================= 2. 列名智能映射引擎（增强版） =================
COLUMN_RULES = {
    'MSKU': {'exact': ['MSKU', '商家SKU', 'Merchant SKU'], 'fuzzy': ['msku'], 'exclude': ['FNSKU']},
    'SKU': {'exact': ['SKU', 'FNSKU', '子SKU'], 'fuzzy': ['sku'], 'exclude': ['MSKU']},
    'ASIN': {'exact': ['ASIN', '子ASIN', 'Child ASIN'], 'fuzzy': ['asin']},
    '店铺': {'exact': ['店铺', 'Shop', 'Store', 'Account', '账号'], 'fuzzy': ['店铺', 'shop']},
    'SPU': {'exact': ['SPU', '父ASIN', 'Parent ASIN'], 'fuzzy': ['spu', '父asin']},
}

def smart_find_column(df, key):
    if key not in COLUMN_RULES:
        return None
    rule = COLUMN_RULES[key]
    cols_lower = {c: c.lower() for c in df.columns}
    for exact in rule.get('exact', []):
        for orig, low in cols_lower.items():
            if exact.lower() == low:
                return orig
    exclude_words = [e.lower() for e in rule.get('exclude', [])]
    for fuzzy in rule.get('fuzzy', []):
        for orig, low in cols_lower.items():
            if fuzzy.lower() in low and not any(ex in low for ex in exclude_words):
                return orig
    return None

def find_col(df, exacts, fuzzys=None):
    """增强版列名查找：支持多语言变体"""
    if fuzzys is None:
        fuzzys = []
    # 精确匹配优先
    for kw in exacts:
        for c in df.columns:
            if kw.lower() == str(c).lower():
                return c
    # 模糊匹配（忽略大小写）
    for kw in fuzzys + exacts:  # exacts 也纳入模糊搜索以覆盖大小写变体
        for c in df.columns:
            if kw.lower() in str(c).lower():
                return c
    return None

# ================= 3. 基础数据清洗函数 =================
def deduplicate_uploaded_files(files):
    seen, unique = set(), []
    for f in files:
        if f.name not in seen:
            seen.add(f.name)
            unique.append(f)
    return unique

@st.cache_data
def clean_msku_strict(val):
    """严格清理 MSKU/SKU：去除所有空白和不可见字符"""
    if pd.isna(val):
        return ""
    # 移除所有空白字符（包括空格、制表符、换行等）以及 ASCII 控制字符
    return re.sub(r'[\s\x00-\x1f\x7f]', '', str(val).strip())

def to_numeric_fast(series):
    s = series.astype(str).str.strip().replace(['-', 'nan', 'NaN', 'None', ''], '0')
    has_pct = s.str.contains('%', na=False)
    s = s.str.replace(r'[^\d.-]', '', regex=True)
    s = pd.to_numeric(s, errors='coerce').fillna(0.0)
    s.loc[has_pct] = s.loc[has_pct] / 100.0
    return s

def clean_columns(df):
    df.columns = [str(c).strip() for c in df.columns]
    return df

def read_file(file_obj):
    if file_obj.name.endswith('.csv'):
        try:
            return pd.read_csv(file_obj, encoding='utf-8')
        except UnicodeDecodeError:
            file_obj.seek(0)
            return pd.read_csv(file_obj, encoding='gbk')
    else:
        return pd.read_excel(file_obj)

# ================= 4. 核心处理函数（支持缓存） =================
def get_file_bytes(file_obj):
    file_obj.seek(0)
    return file_obj.read()

@st.cache_data(show_spinner=False)
def process_traffic_cached(file_data_list, prefix):
    all_dfs = []
    for file_bytes, fname in file_data_list:
        try:
            file_obj = io.BytesIO(file_bytes)
            file_obj.name = fname
            df = clean_columns(read_file(file_obj).drop_duplicates())
            sku_col = find_col(df, ['SKU', '子ASIN', '子 SKU'], ['(Child)', 'sku'])
            if not sku_col:
                continue
            df['join_key'] = df[sku_col].apply(clean_msku_strict)
            shop_col = find_col(df, ['店铺', 'Shop', 'Store', 'Account', '账号'], ['店铺', 'Shop', 'Store'])
            df['traffic_shop'] = df[shop_col].astype(str).str.strip() if shop_col else 'Unknown'

            # 放宽的流量指标识别
            indicators = [
                (["会话数", "Sessions", "浏览会话"], ["会话", "session"], ["占比", "转化"], "会话数"),
                (["页面浏览量", "Views", "页面浏览"], ["页面浏览", "view"], ["占比", "转化"], "页面浏览量"),
                (["订单商品总数", "Units Ordered", "订购数量"], ["订单商品", "units"], ["转化"], "订单商品总数"),
                (["销售额", "Product Sales", "销售总额"], ["销售"], ["转化"], "销售额")
            ]
            found = {}
            for exacts, fuzzys, excls, suffix in indicators:
                c_found = find_col(df, exacts, fuzzys)
                if c_found and not any(ex.lower() in c_found.lower() for ex in excls):
                    df[c_found] = to_numeric_fast(df[c_found])
                    found[c_found] = f"{prefix}{suffix}"

            if not found:
                continue
            cols = ['join_key', 'traffic_shop'] + list(found.keys())
            temp = df[cols].groupby(['join_key', 'traffic_shop'], dropna=False).sum().reset_index().rename(columns=found)
            all_dfs.append(temp)
        except Exception as e:
            st.warning(f"⚠️ 处理流量文件 {fname} 时出错：{e}")
    if not all_dfs:
        return None
    combined = pd.concat(all_dfs, ignore_index=True)
    val_cols = [c for c in combined.columns if c not in ['join_key', 'traffic_shop']]
    return combined.groupby(['join_key', 'traffic_shop'], dropna=False)[val_cols].sum().reset_index()

@st.cache_data(show_spinner=False)
def process_inventory_cached(file_data_list):
    all_dfs, today = [], pd.Timestamp.now().normalize()
    for file_bytes, fname in file_data_list:
        try:
            file_obj = io.BytesIO(file_bytes)
            file_obj.name = fname
            df = clean_columns(read_file(file_obj).drop_duplicates())
            c_sku = find_col(df, ['SKU'], ['产品'])
            c_qty = find_col(df, ['海外仓在途', '在途数量', '发货量', '数量', 'Qty', '可用数量'], ['在途'])
            c_date = find_col(df, ['预计到货时间', '预计到货', 'ETA', '到货时间', '日期'], ['到货', 'eta'])

            if not (c_sku and c_qty and c_date):
                continue

            df['join_key'] = df[c_sku].apply(clean_msku_strict)
            df['qty_clean'] = to_numeric_fast(df[c_qty])
            df['date_clean'] = pd.to_datetime(df[c_date], errors='coerce')
            df['days_diff'] = (df['date_clean'] - today).dt.days

            df['待发货'] = np.where(df['date_clean'].isna(), df['qty_clean'], 0)
            df['7天内送达'] = np.where(df['date_clean'].notna() & (df['days_diff'] <= 7), df['qty_clean'], 0)
            df['14天内送达'] = np.where(df['date_clean'].notna() & (df['days_diff'] > 7) & (df['days_diff'] <= 14), df['qty_clean'], 0)
            df['21天内送达'] = np.where(df['date_clean'].notna() & (df['days_diff'] > 14) & (df['days_diff'] <= 21), df['qty_clean'], 0)
            df['28天内送达'] = np.where(df['date_clean'].notna() & (df['days_diff'] > 21) & (df['days_diff'] <= 28), df['qty_clean'], 0)
            df['28天以上送达'] = np.where(df['date_clean'].notna() & (df['days_diff'] > 28), df['qty_clean'], 0)
            all_dfs.append(df[['join_key', '待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达']])
        except Exception as e:
            st.warning(f"⚠️ 处理库存文件 {fname} 时出错：{e}")
    if not all_dfs:
        return None
    combined = pd.concat(all_dfs, ignore_index=True)
    return combined.groupby('join_key').sum().reset_index()

@st.cache_data(show_spinner=False)
def process_age_cached(file_data_list):
    all_dfs = []
    targets = [("可用量", "可用量"), ("0~30库龄", "0~30库龄"), ("31~60库龄", "31~60库龄"), ("61~90库龄", "61~90库龄"),
               ("91~180库龄", "91~180库龄"), ("181~270库龄", "181~270库龄"), ("271~330库龄", "271~330库龄"),
               ("331~365库龄", "331~365库龄"), ("365以上库龄", "365以上库龄"), ("181以上库龄", "181以上库龄")]
    for file_bytes, fname in file_data_list:
        try:
            file_obj = io.BytesIO(file_bytes)
            file_obj.name = fname
            df = clean_columns(read_file(file_obj).drop_duplicates())
            c_sku = find_col(df, ['SKU'], ['产品'])
            if not c_sku:
                continue
            res = pd.DataFrame({'join_key': df[c_sku].apply(clean_msku_strict)})

            c_age = find_col(df, ['库龄', 'Age'], ['库龄', 'Age'])
            c_qty = find_col(df, ['海外仓在库', '可用', '可用量', '在库数量'], ['可用'])

            if c_age and c_qty and not find_col(df, ["0~30", "0-30"]):
                qty, age = to_numeric_fast(df[c_qty]), to_numeric_fast(df[c_age])
                res['可用量'] = qty
                res['0~30库龄'] = np.where((age >= 0) & (age <= 30), qty, 0)
                res['31~60库龄'] = np.where((age > 30) & (age <= 60), qty, 0)
                res['61~90库龄'] = np.where((age > 60) & (age <= 90), qty, 0)
                res['91~180库龄'] = np.where((age > 90) & (age <= 180), qty, 0)
                res['181~270库龄'] = np.where((age > 180) & (age <= 270), qty, 0)
                res['271~330库龄'] = np.where((age > 270) & (age <= 330), qty, 0)
                res['331~365库龄'] = np.where((age > 330) & (age <= 365), qty, 0)
                res['365以上库龄'] = np.where(age > 365, qty, 0)
                res['181以上库龄'] = np.where(age > 180, qty, 0)
            else:
                for pat, t_name in targets:
                    rc = find_col(df, [pat])
                    res[t_name] = to_numeric_fast(df[rc]) if rc else 0
            all_dfs.append(res)
        except Exception as e:
            st.warning(f"⚠️ 处理库龄文件 {fname} 时出错：{e}")
    if not all_dfs:
        return None
    combined = pd.concat(all_dfs, ignore_index=True)
    return combined.groupby('join_key').sum().reset_index()

# ================= 5. 侧边栏 =================
with st.sidebar:
    st.header("📋 1. SOP 命名规范")
    st.markdown("""<div class="sop-box"><b>⚠️ 警告：不规范文件将被直接拒收。</b><br>
    ✅ <code>产品表现_</code> / <code>白名单_</code><br>✅ <code>7天流量_</code><br>✅ <code>14天流量_</code><br>✅ <code>库存_</code> / <code>海外仓_</code><br>✅ <code>库龄_</code></div>""", unsafe_allow_html=True)
    st.write("")
    st.header("📁 2. 一键拖拽区")
    all_files = st.file_uploader("全选所有表并拖入", accept_multiple_files=True, label_visibility="collapsed")
    st.header("⚙️ 3. 运营参数")
    TARGET_DAYS_TRANSIT = st.number_input("在途目标天数", value=60)
    TARGET_DAYS_STOCK = st.number_input("库存目标天数", value=30)
    TARGET_DAYS_TOTAL = TARGET_DAYS_TRANSIT + TARGET_DAYS_STOCK
    ALERT_STOCKOUT_DAYS = st.number_input("断货红线 (预警)", value=15)
    run_btn = st.button("🚀 开始全息穿透分析", type="primary", use_container_width=True)

# ================= 6. 主运算逻辑 =================
if run_btn:
    if not all_files:
        st.error("❌ 请上传数据表格！")
    else:
        unique_files = deduplicate_uploaded_files(all_files)
        f_prod, f_7d, f_14d, f_inv, f_age, unrec = [], [], [], [], [], []
        for f in unique_files:
            fname = f.name.lower()
            if "7天流量" in fname or "traffic_7d" in fname:
                f_7d.append(f)
            elif "14天流量" in fname or "traffic_14d" in fname:
                f_14d.append(f)
            elif "库龄" in fname or "age" in fname:
                f_age.append(f)
            elif "海外仓" in fname or "库存" in fname or "inventory" in fname:
                f_inv.append(f)
            elif "产品表现" in fname or "白名单" in fname or "重要" in fname or "product" in fname:
                f_prod.append(f)
            else:
                unrec.append(f.name)

        if unrec:
            st.warning(f"⚠️ 已忽略未按规范命名的文件：{', '.join(unrec)}")
        if not f_prod:
            st.error("❌ 严重错误：未识别到【产品表现表】！")
            st.stop()

        with st.spinner("🧠 正在执行引擎重构，挂载多维数据穿透与排雷网..."):
            f_7d_data = [(get_file_bytes(f), f.name) for f in f_7d]
            f_14d_data = [(get_file_bytes(f), f.name) for f in f_14d]
            f_inv_data = [(get_file_bytes(f), f.name) for f in f_inv]
            f_age_data = [(get_file_bytes(f), f.name) for f in f_age]

            # 处理产品表现表和白名单
            df_wl, all_data_dfs = None, []
            for f in f_prod:
                df = clean_columns(read_file(f).drop_duplicates())
                if "重要" in f.name or "白名单" in f.name:
                    c_msku = smart_find_column(df, 'MSKU') or find_col(df, ['MSKU', '商家SKU'], ['sku'])
                    if c_msku:
                        df['MSKU'] = df[c_msku].apply(clean_msku_strict)
                        c_shop = smart_find_column(df, '店铺') or find_col(df, ['店铺', 'Shop', 'Store'], ['店铺', 'Shop'])
                        if c_shop:
                            df['店铺'] = df[c_shop].astype(str).str.strip()
                            df_wl = df[['MSKU', '店铺']].drop_duplicates()
                        else:
                            df_wl = df[['MSKU']].drop_duplicates()
                    continue

                c_msku = smart_find_column(df, 'MSKU') or find_col(df, ['MSKU', '商家SKU'], ['MSKU', 'sku'])
                c_sku = smart_find_column(df, 'SKU') or find_col(df, ['SKU', 'FNSKU'], ['SKU', 'FNSKU'])
                if c_msku:
                    df['MSKU'] = df[c_msku].apply(clean_msku_strict)
                    df['SKU_KEY'] = df[c_sku].apply(clean_msku_strict) if c_sku else df['MSKU']
                    c_shop = smart_find_column(df, '店铺') or find_col(df, ['店铺', 'Shop', 'Store'], ['店铺', 'Shop'])
                    if c_shop:
                        df['店铺'] = df[c_shop].astype(str).str.strip()
                    c_asin = smart_find_column(df, 'ASIN') or find_col(df, ['ASIN', '子ASIN'], ['ASIN'])
                    if c_asin and c_asin != 'ASIN':
                        df.rename(columns={c_asin: 'ASIN'}, inplace=True)
                    all_data_dfs.append(df)

            if not all_data_dfs:
                st.error("❌ 找不到 MSKU 列！")
                st.stop()
            df_master = pd.concat(all_data_dfs, ignore_index=True).drop_duplicates()

            # 白名单过滤
            if df_wl is not None:
                if '店铺' in df_wl.columns and '店铺' in df_master.columns:
                    def normalize_shop(s):
                        return str(s).upper().replace(" ", "").replace("AMAZON", "").strip()
                    df_master['_shop_norm'] = df_master['店铺'].apply(normalize_shop)
                    df_wl['_shop_norm'] = df_wl['店铺'].apply(normalize_shop)
                    merged = pd.merge(df_master, df_wl[['MSKU', '_shop_norm']], on='MSKU', how='inner')
                    mask = merged.apply(lambda row: (row['_shop_norm_y'] in row['_shop_norm_x']) or (row['_shop_norm_x'] in row['_shop_norm_y']), axis=1)
                    df_master = merged[mask].drop(columns=['_shop_norm_x', '_shop_norm_y'], errors='ignore')
                else:
                    df_master = pd.merge(df_master, df_wl[['MSKU']].drop_duplicates(), on='MSKU', how='inner')
            if df_master.empty:
                st.error("❌ 白名单过滤后无数据！")
                st.stop()

            # ========== 统一数值列（增强广告列映射） ==========
            standard_columns = {
                '7天销售额': (['7天销售额', '销售额(7天)', '7 days sales', '7天销售'], ['7天销售']),
                '14天销售额': (['14天销售额', '销售额(14天)', '14 days sales', '14天销售'], ['14天销售']),
                '7天订单商品总数': (['7天订单商品总数', '7天订单', '订单(7天)', '7天销量'], ['7天销量', 'units ordered 7d']),
                '14天订单商品总数': (['14天订单商品总数', '14天订单', '订单(14天)', '14天销量'], ['14天销量']),
                '广告花费': (['广告花费', 'Spend', 'Ad Spend', '花费', '广告支出', '广告花费(USD)'], ['ad spend', 'spend']),
                '广告销售额': (['广告销售额', 'Ad Sales', '广告销售', 'Attributed Sales', '广告销售额(USD)'], ['ad sales', 'attributed sales']),
                '广告订单': (['广告订单量', '广告订单', 'Ad Orders', 'Orders'], ['ad orders']),
                '广告点击数': (['广告点击数', '广告点击', 'Ad Clicks', 'Clicks'], ['ad clicks']),
                '广告曝光量': (['广告曝光量', '广告展示量', '广告展示', '广告曝光', 'Impressions'], ['ad impressions']),
                '订单毛利润': (['订单毛利润', '毛利润', '毛利额', 'Gross Profit'], ['profit', 'gross'])
            }

            for std, (exacts, fuzzys) in standard_columns.items():
                found = find_col(df_master, exacts, fuzzys)
                if found and found != std:
                    df_master.rename(columns={found: std}, inplace=True)
                elif not found:
                    df_master[std] = 0.0
                # 确保转换为数值
                if std in df_master.columns:
                    df_master[std] = to_numeric_fast(df_master[std])

            # 调用缓存处理流量、库存、库龄
            df_7 = process_traffic_cached(f_7d_data, "7天")
            df_14 = process_traffic_cached(f_14d_data, "14天")
            df_inventory = process_inventory_cached(f_inv_data)
            df_age = process_age_cached(f_age_data)

            # 调试输出（可选，验证流量数据是否被正确加载）
            if st.secrets.get("debug_mode", False):
                if df_7 is not None:
                    st.write("7天流量表样例", df_7.head(3))
                if df_14 is not None:
                    st.write("14天流量表样例", df_14.head(3))

            # 向量化合并流量表（修正店铺匹配过于严格的问题）
            def merge_traffic_vectorized(m_df, t_df):
                if t_df is None or t_df.empty:
                    return m_df
                t_new_cols = [c for c in t_df.columns if c not in m_df.columns and c not in ['join_key', 'traffic_shop']]
                if not t_new_cols:
                    return m_df
                merged = pd.merge(m_df, t_df[['join_key', 'traffic_shop'] + t_new_cols],
                                  left_on='MSKU', right_on='join_key', how='left')
                # 移除严格的店铺匹配清零逻辑，改为保留所有数据，因为店铺列在主表中可能已规范化
                # 原始代码中的 mask_mismatch 会将不匹配店铺的数据清零，注释掉以避免数据丢失
                # mask_mismatch = (merged['traffic_shop'].fillna('').str.upper().str.replace(' ', '') !=
                #                  merged['店铺'].fillna('').str.upper().str.replace(' ', ''))
                # merged.loc[mask_mismatch, t_new_cols] = 0
                return merged.drop(columns=['join_key', 'traffic_shop'], errors='ignore')

            merged = df_master.copy()
            merged = merge_traffic_vectorized(merged, df_7)
            merged = merge_traffic_vectorized(merged, df_14)

            # 聚合多店铺同一MSKU
            num_cols = merged.select_dtypes(include=np.number).columns.tolist()
            text_cols = [c for c in merged.columns if c not in num_cols and c != 'MSKU']
            agg_dict = {c: 'sum' for c in num_cols}
            agg_dict.update({c: 'first' for c in text_cols})
            merged_agg = merged.groupby('MSKU', as_index=False).agg(agg_dict)

            # 合并库存与库龄表
            for df_t, merge_on in [(df_inventory, 'join_key'), (df_age, 'join_key')]:
                if df_t is not None and not df_t.empty:
                    df_t = df_t.loc[:, ~df_t.columns.duplicated()]
                    if df_t['join_key'].duplicated().any():
                        df_t = df_t.groupby('join_key', as_index=False).sum()
                    merged_agg = pd.merge(merged_agg, df_t, left_on='SKU_KEY', right_on=merge_on, how='left').drop(columns=[merge_on], errors='ignore')

            merged_agg = merged_agg.fillna(0)

            # 计算指标
            merged_agg['商品属性'] = merged_agg['MSKU'].apply(lambda x: '二手商品' if 'amzn.gr' in str(x).lower() else '')
            merged_agg['7天日均订单'] = merged_agg.get('7天订单商品总数', 0) / 7
            merged_agg['14天日均订单'] = merged_agg.get('14天订单商品总数', 0) / 14

            inv_c = [c for c in ['待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达'] if c in merged_agg.columns]
            merged_agg['待到合计'] = merged_agg[inv_c].sum(axis=1) if inv_c else 0
            merged_agg['总供给'] = merged_agg.get('可用量', 0) + merged_agg.get('待到合计', 0)

            # 预测日销量与SKU层级需求
            merged_agg['预测日销量'] = (merged_agg['7天日均订单'] + merged_agg['14天日均订单']) / 2
            merged_agg['SKU_总日均'] = merged_agg.groupby('MSKU')['预测日销量'].transform('sum')
            merged_agg['SKU_总供给'] = merged_agg['总供给']  # 已按MSKU唯一

            merged_agg['理论需求量'] = merged_agg['SKU_总日均'] * TARGET_DAYS_TOTAL
            merged_agg['建议补货量'] = np.maximum(0, merged_agg['理论需求量'] - merged_agg['SKU_总供给'])
            merged_agg['预计可售天数'] = np.where(merged_agg['SKU_总日均'] > 0.1, merged_agg['SKU_总供给'] / merged_agg['SKU_总日均'], 999)

            # 比率计算
            def safe_div(n, d):
                return np.where(merged_agg.get(d, 0) > 0, merged_agg.get(n, 0) / merged_agg.get(d, 1), 0)
            merged_agg['ACOS'] = safe_div('广告花费', '广告销售额')
            merged_agg['CPC'] = safe_div('广告花费', '广告点击数')
            merged_agg['ACoAS'] = safe_div('广告花费', '7天销售额')
            merged_agg['广告CVR'] = safe_div('广告订单', '广告点击数')
            merged_agg['CTR'] = safe_div('广告点击数', '广告曝光量')
            merged_agg['订单毛利率'] = safe_div('订单毛利润', '7天销售额')
            merged_agg['7天销售转化率'] = safe_div('7天订单商品总数', '7天会话数')
            merged_agg['14天销售转化率'] = safe_div('14天订单商品总数', '14天会话数')

            merged_agg.drop(columns=['SKU_KEY'], errors='ignore', inplace=True)

            # 展示层去重：创建副本，不污染原始数据
            df_display = merged_agg.sort_values(by=['MSKU', '7天销售额'], ascending=[True, False]).reset_index(drop=True)
            is_dup = df_display.duplicated(subset=['MSKU'], keep='first')
            clr_cols = ['可用量', '待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达',
                        '待到合计', '总供给', '建议补货量', '0~30库龄', '31~60库龄', '61~90库龄', '91~180库龄',
                        '181~270库龄', '271~330库龄', '331~365库龄', '365以上库龄', '181以上库龄', '预计可售天数', '理论需求量']
            for c in [c for c in clr_cols if c in df_display.columns]:
                df_display.loc[is_dup, c] = 0
            df_display.loc[is_dup, '预计可售天数'] = None

            # 列排序
            mv_front = [c for c in ['预测日销量', '建议补货量', '预计可售天数', '理论需求量', '总供给'] if c in df_display.columns]
            others = [c for c in df_display.columns if c not in mv_front]
            cols = others[:12] + mv_front + others[12:]

            def move_col(lst, c, r, pos='before'):
                if c in lst and r in lst:
                    lst.remove(c)
                    lst.insert(lst.index(r) + (0 if pos == 'before' else 1), c)
                return lst
            cols = move_col(cols, '店铺', 'MSKU', 'before')
            if 'ASIN' in cols:
                cols = move_col(cols, 'ASIN', 'MSKU', 'after')
            for a, b, p in [('7天日均订单', '7天销售额', 'before'), ('7天销售转化率', '7天销售额', 'after'),
                            ('14天日均订单', '14天销售额', 'before'), ('14天销售转化率', '14天销售额', 'after'),
                            ('待到合计', '7天内送达', 'before'), ('待发货', '可用量', 'before'),
                            ('广告点击数', '广告花费', 'before'), ('广告销售额', '广告花费', 'after'),
                            ('广告曝光量', '广告点击数', 'before'), ('广告订单', '广告销售额', 'before'),
                            ('商品属性', 'ASIN', 'after')]:
                cols = move_col(cols, a, b, p)
            merged_final = df_display[cols]

            # Excel 生成
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                merged_final.to_excel(writer, index=False, sheet_name='补货数据')
                ws = writer.sheets['补货数据']
                ws.insert_rows(1, amount=2)
                ws.freeze_panes = 'J4'
                ws['A1'], ws['A2'] = "总计求和", "筛选求和"

                f_g, f_h = Font(name='Arial', size=11), Font(name='Arial', size=11, bold=True)
                a_b, a_h = Alignment(vertical='center'), Alignment(horizontal='center', vertical='center', wrap_text=True)
                bd = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
                c_r, c_y, c_b, c_g = [PatternFill(start_color=c, end_color=c, fill_type="solid") for c in ["FFC7CE", "FFEB9C", "BDD7EE", "C6EFCE"]]
                i2n = {cell.col_idx: str(cell.value).strip() for cell in ws[3] if cell.value}

                ly, lb, lg, lfy = ['店铺', '商品属性', 'ASIN', 'SKU', '父ASIN', 'SPU', '一级分类', '二级分类', '三级分类', '型号'], \
                                  ['订单毛利润', '订单毛利率'], \
                                  ['广告点击数', '广告曝光量', '广告订单', '广告花费', '广告销售额', 'CTR', '广告CVR', 'CPC', 'ACOS', 'ACoAS', '7天会话数', '7天页面浏览量', '7天订单商品总数', '7天日均订单', '7天销售额', '7天销售转化率', '14天会话数', '14天页面浏览量', '14天订单商品总数', '14天日均订单', '14天销售额', '14天销售转化率'], \
                                  ['待到合计', '可用量', '待发货']
                idx_sd = next((k for k, v in i2n.items() if v == '预计可售天数'), None)

                for r in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
                    is_risk = (r[0].row >= 4 and idx_sd and isinstance(r[idx_sd-1].value, (int, float)) and r[idx_sd-1].value < ALERT_STOCKOUT_DAYS)
                    for cell in r:
                        ro, co, val, cname = cell.row, cell.col_idx, cell.value, i2n.get(cell.col_idx, "")
                        cell.font, cell.alignment = (f_h, a_h if ro == 3 else a_b) if ro <= 3 else (f_g, a_b)
                        if val is not None or ro == 3 or co <= 9:
                            cell.border = bd

                        if isinstance(val, (int, float)):
                            if any(x in cname for x in ["率", "CTR", "ACOS", "ACoAS", "CVR", "占比"]):
                                cell.number_format = '0.00%'
                            elif any(x in cname for x in ["送达", "待到", "可用", "库龄", "补货", "供给", "需求", "数量", "待发货", "点击", "曝光", "展示", "会话", "浏览"]):
                                cell.number_format = '#,##0'
                            elif "天数" in cname:
                                cell.number_format = '0.0'
                            else:
                                cell.number_format = '#,##0.00'

                        if ro == 3:
                            if cname in ly or cname in lfy:
                                cell.fill = c_y
                            elif cname in lb:
                                cell.fill = c_b
                            elif cname in lg or any(x in cname for x in ['点击', '曝光', '展示']):
                                cell.fill = c_g
                        elif ro >= 4:
                            if cname in lb:
                                cell.fill = c_b
                            elif cname in lfy:
                                cell.fill = c_y
                            if is_risk and cname in ['MSKU', 'ASIN', '可用量', '建议补货量', '预计可售天数'] and (isinstance(val, str) or (isinstance(val, (int, float)) and val > 0)):
                                cell.fill = c_r
                            if isinstance(val, (int, float)) and val > 0:
                                if any(x in cname for x in ['91~180库龄', '181~270库龄', '271~330库龄', '331~365库龄', '365以上库龄', '181以上库龄']):
                                    cell.fill = c_r
                                elif cname == '61~90库龄':
                                    cell.fill = c_y

                for col in ws.columns:
                    max_len = 0
                    col_letter = get_column_letter(col[0].column)
                    for cell in col[: min(ws.max_row, 500)]:
                        try:
                            if cell.value:
                                val_len = sum(2.0 if '\u4e00' <= char <= '\u9fa5' else 1.1 for char in str(cell.value))
                                max_len = max(max_len, val_len)
                        except:
                            pass
                    ws.column_dimensions[col_letter].width = min(max_len + 3, 35)

                ws.row_dimensions[3].height = 40

                cm_r = {v: get_column_letter(k) for k, v in i2n.items()}
                for c in range(5, ws.max_column + 1):
                    cn = i2n.get(c, "")
                    if any(k in cn for k in ['销量', '销售额', '数量', '会话', '浏览', '送达', '待到', '可用', '库龄', '补货', '供给', '需求', '利润', '广告', '花费', '订单', '待发货', '点击', '曝光']) and \
                       not any(ex in cn for ex in ['店铺', 'MSKU', 'ASIN', 'SKU', '图片', 'image', '转化率', '可售天数', '商品属性', '分类', '型号']):
                        cl, rs = get_column_letter(c), f"{get_column_letter(c)}4:{get_column_letter(c)}{ws.max_row}"
                        ws[f'{cl}1'], ws[f'{cl}2'] = f"=SUM({rs})", f"=SUBTOTAL(109, {rs})"
                        for ri in [1, 2]:
                            ws[f'{cl}{ri}'].font, ws[f'{cl}{ri}'].border = f_h, bd
                            ws[f'{cl}{ri}'].number_format = '#,##0' if any(x in cn for x in ["送达", "待到", "可用", "库龄", "补货", "供给", "需求", "数量", "待发货", "点击", "曝光", "展示", "会话", "浏览"]) else '#,##0.00'

                for kw, num, den, fmt in [('订单毛利率', '订单毛利润', '7天销售额', '0.00%'), ('ACoAS', '广告花费', '7天销售额', '0.00%'),
                                          ('ACOS', '广告花费', '广告销售额', '0.00%'), ('CPC', '广告花费', '广告点击数', '#,##0.00'),
                                          ('广告CVR', '广告订单', '广告点击数', '0.00%'), ('7天销售转化率', '7天订单商品总数', '7天会话数', '0.00%'),
                                          ('14天销售转化率', '14天订单商品总数', '14天会话数', '0.00%'), ('CTR', '广告点击数', '广告曝光量', '0.00%')]:
                    for c, cn in i2n.items():
                        if kw.upper() in cn.upper():
                            c_n, c_d = cm_r.get(num), cm_r.get(den)
                            if c_n and c_d:
                                cl = get_column_letter(c)
                                ws[f'{cl}1'], ws[f'{cl}2'] = f"=IFERROR(SUM({c_n}4:{c_n}{ws.max_row})/SUM({c_d}4:{c_d}{ws.max_row}),0)", f"=IFERROR(SUBTOTAL(109,{c_n}4:{c_n}{ws.max_row})/SUBTOTAL(109,{c_d}4:{c_d}{ws.max_row}),0)"
                                for ri in [1, 2]:
                                    ws[f'{cl}{ri}'].number_format, ws[f'{cl}{ri}'].font, ws[f'{cl}{ri}'].border = fmt, f_h, bd

        st.session_state.processed_excel = output.getvalue()
        st.session_state.df_vis = merged_final

# ================= 7. 可视化与筛选 =================
if "df_vis" in st.session_state:
    df_vis = st.session_state.df_vis.loc[:, ~st.session_state.df_vis.columns.duplicated()].copy()
    st.markdown("---")

    c_t1, c_t2, c_t3, c_t4, c_t5 = st.columns([0.8, 1, 1, 1, 1.2])
    with c_t1:
        st.markdown("##### ⏱️ 分析周期")
        prefix = "7天" if "7天" in st.radio("周期", ["7天数据表现", "14天数据表现"], horizontal=True, label_visibility="collapsed") else "14天"

    df_f = df_vis.copy()

    with c_t2:
        st.markdown("##### 🏢 店铺")
        opt_s = sorted(list(set([p.strip() for s in df_f['店铺'].dropna() for p in str(s).split('|') if p.strip()]))) if '店铺' in df_f.columns else []
        sel_s = st.multiselect("店", opt_s, label_visibility="collapsed")
        if sel_s:
            df_f = df_f[df_f['店铺'].apply(lambda x: any(s in str(x) for s in sel_s))]

    spu_c = smart_find_column(df_f, 'SPU') or find_col(df_f, ['SPU', '父ASIN'])
    with c_t3:
        st.markdown("##### 🔗 SPU")
        opt_p = sorted(df_f[spu_c].dropna().unique().tolist()) if spu_c else []
        sel_p = st.multiselect("S", opt_p, label_visibility="collapsed")
        if sel_p and spu_c:
            df_f = df_f[df_f[spu_c].isin(sel_p)]

    with c_t4:
        st.markdown("##### 🏷️ ASIN")
        opt_a = sorted(df_f['ASIN'].dropna().unique().tolist()) if 'ASIN' in df_f.columns else []
        sel_a = st.multiselect("A", opt_a, label_visibility="collapsed")
        if sel_a:
            df_f = df_f[df_f['ASIN'].isin(sel_a)]

    with c_t5:
        st.markdown("##### 📦 全息穿透 MSKU")
        opt_m = sorted(df_f['MSKU'].dropna().unique().tolist()) if 'MSKU' in df_f.columns else []
        sel_m = st.multiselect("M", opt_m, label_visibility="collapsed")
        if sel_m:
            df_f = df_f[df_f['MSKU'].isin(sel_m)]

    target_df = df_f
    is_filtered = bool(sel_s or sel_p or sel_a or sel_m)

    def s_sum(df, col):
        return float(pd.to_numeric(df.get(col, 0), errors='coerce').sum())

    def calc_kpi(df, period):
        sales_col = f'{period}销售额'
        profit_col = '订单毛利润'
        ad_cost_col = '广告花费'
        ad_sales_col = '广告销售额'
        supply_col = '总供给'
        demand_col = '预测日销量'

        sa = s_sum(df, sales_col)
        pr = s_sum(df, profit_col)
        ad = s_sum(df, ad_cost_col)
        sp = s_sum(df, supply_col)
        da = s_sum(df, demand_col)

        gross_margin = pr / sa if sa > 0 else 0
        acoas = ad / sa if sa > 0 else 0
        ad_ratio_profit = ad / pr if pr > 0 else 0
        stock_days = sp / da if da > 0 else 0
        return pr, gross_margin, acoas, ad_ratio_profit, sa, stock_days

    g_p, g_m, g_ac, g_ap, g_s, g_sd = calc_kpi(df_vis, '7天')
    t_p, t_m, t_ac, t_ap, t_s, t_sd = calc_kpi(target_df, prefix)

    st.markdown("##### 💡 核心 KPI 指标")
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    def d_met(c, l, v, gv=None, r=None):
        if is_filtered and r is not None:
            c.metric(l, v, f"占大盘 {r:.1%}", delta_color="off")
        elif is_filtered and gv is not None:
            c.metric(l, v, f"大盘 {gv}", delta_color="off")
        else:
            c.metric(l, v)

    d_met(m1, "💰 1.毛利额", f"${t_p:,.2f}", r=t_p/g_p if g_p else 0)
    d_met(m2, "📈 2.毛利率", f"{t_m:.2%}", gv=f"{g_m:.2%}")
    d_met(m3, "🔥 3.广告费比", f"{t_ac:.2%}", gv=f"{g_ac:.2%}")
    d_met(m4, "⚖️ 4.广告占毛利", f"{t_ap:.2%}", gv=f"{g_ap:.2%}")
    d_met(m5, f"🛒 5.销售额({prefix})", f"${t_s:,.2f}", r=t_s/g_s if g_s else 0)
    d_met(m6, "📦 6.可售天数", f"{t_sd:,.1f}天", gv=f"{g_sd:,.1f}天")

    tab1, tab2 = st.tabs([f"🔗 销售与排行 ({prefix})", f"📦 库存与气泡阵 ({prefix})"])
    with tab1:
        if not spu_c:
            st.warning("⚠️ 未找到 SPU 列")
        else:
            fmt_d = {f'{prefix}销售额': '${:,.2f}', '订单毛利润': '${:,.2f}', '广告花费': '${:,.2f}', '广告销售额': '${:,.2f}',
                     'CPC': '${:,.2f}', '订单毛利率': '{:.2%}', 'ACOS': '{:.2%}', 'ACoAS': '{:.2%}', '广告CVR': '{:.2%}',
                     'CTR': '{:.2%}', f'{prefix}销售转化率': '{:.2%}', '可用量': '{:,.0f}', '待到合计': '{:,.0f}',
                     '建议补货量': '{:,.0f}', f'{prefix}日均订单': '{:,.2f}', '预计可售天数': '{:,.1f}'}
            def _sd(n, d):
                return s_sum(target_df, n) / s_sum(target_df, d) if s_sum(target_df, d) > 0 else 0

            st.markdown("##### 📝 链接明细概览表")
            if is_filtered:
                s_cols = [c for c in ['MSKU', spu_c, 'ASIN', f'{prefix}销售额', '订单毛利润', '订单毛利率', '广告花费', '广告销售额',
                                      'ACOS', 'ACoAS', 'CPC', '广告CVR', 'CTR', f'{prefix}销售转化率', f'{prefix}日均订单',
                                      '可用量', '待到合计', '预计可售天数', '建议补货量'] if c in target_df.columns]
                t_row = pd.Series(index=s_cols, dtype=object)
                t_row['MSKU'] = '🌟 已选合计'
                for c in [spu_c, 'ASIN']:
                    if c in s_cols:
                        t_row[c] = '-'
                for c in [f'{prefix}销售额', '订单毛利润', '广告花费', '广告销售额', '可用量', '待到合计', '建议补货量', f'{prefix}日均订单']:
                    if c in s_cols:
                        t_row[c] = pd.to_numeric(target_df.get(c, 0), errors='coerce').sum()
                if '订单毛利率' in s_cols:
                    t_row['订单毛利率'] = _sd('订单毛利润', f'{prefix}销售额')
                if 'ACOS' in s_cols:
                    t_row['ACOS'] = _sd('广告花费', '广告销售额')
                if 'ACoAS' in s_cols:
                    t_row['ACoAS'] = _sd('广告花费', f'{prefix}销售额')
                if 'CPC' in s_cols:
                    t_row['CPC'] = _sd('广告花费', '广告点击数')
                if '广告CVR' in s_cols:
                    t_row['广告CVR'] = _sd('广告订单', '广告点击数')
                if 'CTR' in s_cols:
                    t_row['CTR'] = _sd('广告点击数', '广告曝光量')
                if f'{prefix}销售转化率' in s_cols:
                    t_row[f'{prefix}销售转化率'] = _sd(f'{prefix}订单商品总数', f'{prefix}会话数')
                if '预计可售天数' in s_cols:
                    t_row['预计可售天数'] = _sd('总供给', '预测日销量')
                st.dataframe(pd.concat([t_row.to_frame().T, target_df[s_cols]], ignore_index=True).style.format(fmt_d), height=350, use_container_width=True)
            else:
                sc = [f'{prefix}销售额', '订单毛利润', '可用量', '待到合计', '建议补货量', '广告花费', '广告销售额', '广告订单', '广告点击数', '广告曝光量',
                      f'{prefix}订单商品总数', f'{prefix}会话数', f'{prefix}日均订单', '7天销售额', '总供给', '预测日销量']
                sc = list(set([c for c in sc if c in df_vis.columns]))
                for c in sc:
                    df_vis[c] = pd.to_numeric(df_vis[c], errors='coerce').fillna(0)
                sp_df = df_vis.groupby(spu_c, dropna=False)[sc].sum().reset_index()
                sp_df = sp_df[sp_df[spu_c].astype(str).str.strip() != '']

                sp_df['订单毛利率'] = np.where(sp_df.get('7天销售额', 0) > 0, sp_df.get('订单毛利润', 0) / sp_df.get('7天销售额', 1), 0)
                sp_df['ACOS'] = np.where(sp_df.get('广告销售额', 0) > 0, sp_df.get('广告花费', 0) / sp_df.get('广告销售额', 1), 0)
                sp_df['ACoAS'] = np.where(sp_df.get(f'{prefix}销售额', 0) > 0, sp_df.get('广告花费', 0) / sp_df.get(f'{prefix}销售额', 1), 0)
                sp_df['CPC'] = np.where(sp_df.get('广告点击数', 0) > 0, sp_df.get('广告花费', 0) / sp_df.get('广告点击数', 1), 0)
                sp_df['广告CVR'] = np.where(sp_df.get('广告点击数', 0) > 0, sp_df.get('广告订单', 0) / sp_df.get('广告点击数', 1), 0)
                sp_df['CTR'] = np.where(sp_df.get('广告曝光量', 0) > 0, sp_df.get('广告点击数', 0) / sp_df.get('广告曝光量', 1), 0)
                sp_df[f'{prefix}销售转化率'] = np.where(sp_df.get(f'{prefix}会话数', 0) > 0, sp_df.get(f'{prefix}订单商品总数', 0) / sp_df.get(f'{prefix}会话数', 1), 0)
                sp_df['预计可售天数'] = np.where(sp_df.get('预测日销量', 0) > 0, sp_df.get('总供给', 0) / sp_df.get('预测日销量', 1), 0)

                sh_c = [c for c in [spu_c, f'{prefix}销售额', '订单毛利润', '订单毛利率', '广告花费', '广告销售额', 'ACOS', 'ACoAS', 'CPC', '广告CVR', 'CTR', f'{prefix}销售转化率', f'{prefix}日均订单', '可用量', '待到合计', '预计可售天数', '建议补货量'] if c in sp_df.columns]
                st.dataframe(sp_df[sh_c].style.format(fmt_d), height=350, use_container_width=True)

    with tab2:
        c_c1, c_c2 = st.columns([1, 1.5])
        with c_c1:
            st.markdown(f"#### 🚨 断货预警 TOP 15 {'(当前穿透)' if is_filtered else '(全盘)'}")
            if '预计可售天数' in target_df.columns:
                risk_df = target_df[(pd.to_numeric(target_df['预计可售天数'], errors='coerce') < ALERT_STOCKOUT_DAYS) &
                                    (pd.to_numeric(target_df['预测日销量'], errors='coerce') > 0.1)].nsmallest(15, '预计可售天数').sort_values('预计可售天数')
                if not risk_df.empty:
                    fg1 = px.bar(risk_df, x='预计可售天数', y='MSKU', orientation='h', color='建议补货量',
                                 color_continuous_scale='Reds', text_auto='.1f')
                    fg1.update_layout(yaxis={'categoryorder': 'total descending'}, xaxis_title="剩余天数",
                                      margin=dict(l=0, r=0, t=0, b=0), height=400)
                    st.plotly_chart(fg1, use_container_width=True)
                else:
                    st.success("十分安全！无断货风险。")
        with c_c2:
            st.markdown(f"#### 🎯 SKU 表现矩阵 (气泡大小: 销量 | 颜色: ACOS)")
            if f'{prefix}销售额' in target_df.columns:
                m_a = target_df[pd.to_numeric(target_df[f'{prefix}销售额'], errors='coerce') > 0].copy()
                if not m_a.empty:
                    m_a['订单毛利率'] = pd.to_numeric(m_a.get('订单毛利率', 0), errors='coerce')
                    m_a['预计可售天数'] = pd.to_numeric(m_a.get('预计可售天数', 0), errors='coerce').round(1)
                    fg2 = px.scatter(m_a, x=f'{prefix}销售额', y='订单毛利率',
                                     size=pd.to_numeric(m_a.get(f'{prefix}日均订单', 1), errors='coerce').clip(lower=1),
                                     color='ACOS' if 'ACOS' in m_a.columns else None,
                                     hover_name='MSKU', hover_data=['预计可售天数'], size_max=45, color_continuous_scale='RdYlBu_r')
                    am, asls = m_a['订单毛利率'].mean(), m_a[f'{prefix}销售额'].mean()
                    if not pd.isna(am):
                        fg2.add_hline(y=am, line_dash="dot", annotation_text="均毛利")
                    if not pd.isna(asls):
                        fg2.add_vline(x=asls, line_dash="dot", annotation_text="均销额")
                    fg2.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=400)
                    st.plotly_chart(fg2, use_container_width=True)

    st.markdown("---")
    st.download_button(label="📥 下载完整【智能补货分析.xlsx】", data=st.session_state.processed_excel,
                       file_name=f"智能补货分析_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")
