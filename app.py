import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import os
from datetime import datetime
import plotly.express as px
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

# ================= 1. 网页全局配置与密码锁 =================
st.set_page_config(page_title="智能补货与数据中台", page_icon="📦", layout="wide")

TEAM_PASSWORD = "YOETEY2026"

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🔒 内部数据中台 - 安全验证")
    pwd_input = st.text_input("请输入团队专属访问密码：", type="password")
    if st.button("🚀 验证并登录", type="primary"):
        if pwd_input == TEAM_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("❌ 密码错误，请联系管理员获取最新密码。")
    st.stop()

# ================= 下面是核心系统代码 =================
st.markdown("""
    <style>
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    h1, h2, h3, h4, h5 { margin-bottom: 0.1rem !important; padding-bottom: 0.1rem !important; margin-top: 0.1rem !important; }
    div[data-testid="metric-container"] { padding: 0px; margin: 0px; }
    div[data-testid="stMetricValue"] { font-size: 1.4rem !important; font-weight: bold; }
    div[data-testid="stMetricLabel"] { font-size: 0.85rem !important; margin-bottom: -5px; }
    div[data-testid="stMetricDelta"] { font-size: 0.75rem !important; }
    hr { margin-top: 0.5em; margin-bottom: 0.5em; }
    .sop-box { background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 5px solid #ffc107; font-size: 0.9em; color: #333;}
    </style>
""", unsafe_allow_html=True)

st.title("🚀 亚马逊智能补货与分析中台")

def deduplicate_uploaded_files(files):
    if not files: return []
    seen_names = set()
    unique_files = []
    for f in files:
        if f.name not in seen_names:
            seen_names.add(f.name)
            unique_files.append(f)
    return unique_files

@st.cache_data
def clean_msku_strict(val):
    if pd.isna(val): return ""
    return re.sub(r'\s+', '', str(val).strip())

def clean_percentage_or_money(x, col_name=""):
    if pd.isna(x): return 0.0
    s = str(x).strip()
    if s == '-': return 0.0
    try:
        val = float(re.sub(r'[^\d.-]', '', s))
        if any(k in col_name.upper() for k in ['率', 'CTR', 'ACOS', 'ACOAS', 'CVR']):
            if '%' in s or val > 1.0: val = val / 100.0
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

def find_col_exact(df, keyword):
    for col in df.columns:
        if col.upper() == keyword.upper(): return col
    return None

def find_col_by_pattern(df, pattern):
    for col in df.columns:
        if pattern in col: return col
    return None

def read_uploaded_file(f):
    if f.name.endswith('.csv'):
        try: return pd.read_csv(f, encoding='utf-8')
        except: return pd.read_csv(f, encoding='gbk')
    return pd.read_excel(f)

def process_traffic_data(files, prefix):
    if not files: return None
    all_dfs = []
    for f in files:
        try:
            df = read_uploaded_file(f)
            df = clean_columns(df)
            df = df.drop_duplicates()
            
            sku_col = find_col_fuzzy_priority(df, ['SKU', '(Child)', '子ASIN'])
            if not sku_col: continue
            df['join_key'] = df[sku_col].apply(clean_msku_strict)
            shop_col = find_col_fuzzy_priority(df, ['店铺', 'Shop', 'Store', 'Account', '账号'])
            if shop_col: df['traffic_shop'] = df[shop_col].astype(str).str.strip()
            else: df['traffic_shop'] = 'Unknown'
                
            indicators = [
                ("会话", None, "会话数"), ("页面浏览", None, "页面浏览量"),
                ("订单商品", "B2B", "订单商品总数"), ("销售额", "B2B", "销售额")
            ]
            found_data = {}
            for keyword, exclude, suffix in indicators:
                for col in df.columns:
                    if keyword in col:
                        if exclude and exclude in col: continue
                        df[col] = df[col].apply(lambda x: clean_percentage_or_money(x, col))
                        found_data[col] = f"{prefix}{suffix}"
                        break
            
            if not found_data: continue
            cols = ['join_key', 'traffic_shop'] + list(found_data.keys())
            temp = df[cols].groupby(['join_key', 'traffic_shop'], dropna=False).sum().reset_index()
            temp = temp.rename(columns=found_data)
            all_dfs.append(temp)
        except Exception as e: pass
            
    if not all_dfs: return None
    combined = pd.concat(all_dfs, ignore_index=True)
    val_cols = [c for c in combined.columns if c not in ['join_key', 'traffic_shop']]
    return combined.groupby(['join_key', 'traffic_shop'], dropna=False)[val_cols].sum().reset_index()

def process_inventory_data(files):
    if not files: return None
    all_inv_dfs = []
    today = pd.Timestamp.now().normalize()
    for f in files:
        try:
            df = read_uploaded_file(f)
            df = clean_columns(df)
            df = df.drop_duplicates()
            
            col_sku = find_col_fuzzy_priority(df, ['SKU', 'sku', '产品'])
            col_qty = find_col_fuzzy_priority(df, ['海外仓在途', '在途', '发货量', '数量', 'Qty', 'quantity', '件数'])
            col_date = find_col_fuzzy_priority(df, ['实际-预计到货时间', '预计到货', 'XT-预计到货', '实际', '预计', '到货', '时间', '日期', 'ETA'])

            if not (col_sku and col_qty and col_date): continue
            
            df['join_key'] = df[col_sku].apply(clean_msku_strict)
            df['qty_clean'] = df[col_qty].apply(lambda x: clean_percentage_or_money(x, col_qty))
            df['date_clean'] = pd.to_datetime(df[col_date], errors='coerce')
            df['days_diff'] = (df['date_clean'] - today).dt.days
            
            df['待发货'] = df.apply(lambda x: x['qty_clean'] if pd.isna(x['date_clean']) else 0, axis=1)
            df['7天内送达'] = df.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and x['days_diff'] <= 7 else 0, axis=1)
            df['14天内送达'] = df.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and 7 < x['days_diff'] <= 14 else 0, axis=1)
            df['21天内送达'] = df.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and 14 < x['days_diff'] <= 21 else 0, axis=1)
            df['28天内送达'] = df.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and 21 < x['days_diff'] <= 28 else 0, axis=1)
            df['28天以上送达'] = df.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and x['days_diff'] > 28 else 0, axis=1)
            
            all_inv_dfs.append(df[['join_key', '待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达']])
        except: pass
    if not all_inv_dfs: return None
    return pd.concat(all_inv_dfs, ignore_index=True).groupby('join_key').sum().reset_index()

def process_age_data(files):
    if not files: return None
    all_dfs = []
    targets = [("可用", "可用量"), ("0~30", "0~30库龄"), ("31~60", "31~60库龄"), ("61~90", "61~90库龄"), 
               ("91~180", "91~180库龄"), ("181~270", "181~270库龄"), ("271~330", "271~330库龄"), 
               ("331~365", "331~365库龄"), ("365以上", "365以上库龄"), ("181以上", "181以上库龄"), ("181天以上", "181以上库龄")]
    
    for f in files:
        try:
            df = read_uploaded_file(f)
            df = clean_columns(df)
            df = df.drop_duplicates()
            
            col_sku = find_col_fuzzy_priority(df, ['SKU', 'sku', '产品'])
            if not col_sku: continue
            
            df['join_key'] = df[col_sku].apply(clean_msku_strict)
            result_df = pd.DataFrame()
            result_df['join_key'] = df['join_key']
            
            col_age_num = find_col_exact(df, '库龄')
            if not col_age_num: col_age_num = find_col_fuzzy_priority(df, ['库龄', 'Age'])
            col_qty = find_col_fuzzy_priority(df, ['海外仓在库', '可用', '可用量', '在库数量', '数量'])
            has_old_format = find_col_by_pattern(df, "0~30") or find_col_by_pattern(df, "0-30")
            
            if col_age_num and col_qty and not has_old_format:
                df['qty_clean'] = df[col_qty].apply(lambda x: clean_percentage_or_money(x, col_qty))
                df['age_clean'] = df[col_age_num].apply(lambda x: clean_percentage_or_money(x, col_age_num) if pd.notna(x) else 0)
                
                result_df['可用量'] = df['qty_clean']
                result_df['0~30库龄'] = df.apply(lambda x: x['qty_clean'] if 0 <= x['age_clean'] <= 30 else 0, axis=1)
                result_df['31~60库龄'] = df.apply(lambda x: x['qty_clean'] if 30 < x['age_clean'] <= 60 else 0, axis=1)
                result_df['61~90库龄'] = df.apply(lambda x: x['qty_clean'] if 60 < x['age_clean'] <= 90 else 0, axis=1)
                result_df['91~180库龄'] = df.apply(lambda x: x['qty_clean'] if 90 < x['age_clean'] <= 180 else 0, axis=1)
                result_df['181~270库龄'] = df.apply(lambda x: x['qty_clean'] if 180 < x['age_clean'] <= 270 else 0, axis=1)
                result_df['271~330库龄'] = df.apply(lambda x: x['qty_clean'] if 270 < x['age_clean'] <= 330 else 0, axis=1)
                result_df['331~365库龄'] = df.apply(lambda x: x['qty_clean'] if 330 < x['age_clean'] <= 365 else 0, axis=1)
                result_df['365以上库龄'] = df.apply(lambda x: x['qty_clean'] if x['age_clean'] > 365 else 0, axis=1)
                result_df['181以上库龄'] = df.apply(lambda x: x['qty_clean'] if x['age_clean'] > 180 else 0, axis=1)
            else:
                for pattern, target_name in targets:
                    real_col = find_col_by_pattern(df, pattern)
                    if real_col and real_col in df.columns: 
                        result_df[target_name] = df[real_col].apply(lambda x: clean_percentage_or_money(x, target_name))
                    else: result_df[target_name] = 0
            all_dfs.append(result_df)
        except: pass
    if not all_dfs: return None
    return pd.concat(all_dfs, ignore_index=True).groupby('join_key').sum().reset_index()

# ================= 3. 网页侧边栏：SOP看板 & 上传区 =================
with st.sidebar:
    st.header("📋 1. SOP 文件命名规范")
    st.markdown("""
    <div class="sop-box">
    <b>⚠️ 警告：系统将严格核对文件名前缀，不规范文件将被直接拒收。</b><br><br>
    请务必在上传前，将导出的文件名改为以下标准前缀：<br>
    ✅ <code>产品表现_</code> 或 <code>白名单_</code><br>
    ✅ <code>7天流量_</code><br>
    ✅ <code>14天流量_</code><br>
    ✅ <code>库存_</code> 或 <code>海外仓_</code><br>
    ✅ <code>库龄_</code><br>
    <i>示例：7天流量_美区店铺.xlsx</i>
    </div>
    """, unsafe_allow_html=True)
    st.write("") 

    st.header("📁 2. 全选一键拖拽区")
    st.caption("无需分类，直接将所有表格拖入下方虚线框内：")
    all_uploaded_files = st.file_uploader("将所有 Excel/CSV 拖拽至此", accept_multiple_files=True, label_visibility="collapsed")

    st.header("⚙️ 3. 运营参数调整")
    TARGET_DAYS_TRANSIT = st.number_input("在途目标天数", value=60)
    TARGET_DAYS_STOCK = st.number_input("库存目标天数", value=30)
    TARGET_DAYS_TOTAL = TARGET_DAYS_TRANSIT + TARGET_DAYS_STOCK
    ALERT_STOCKOUT_DAYS = st.number_input("断货红线天数 (预警)", value=15)
    
    run_btn = st.button("🚀 开始极速分析大盘", type="primary", use_container_width=True)

# ================= 4. 核心运算与终极Excel排版区 =================
if run_btn:
    if not all_uploaded_files:
        st.error("❌ 请先上传数据表格！")
    else:
        unique_files = deduplicate_uploaded_files(all_uploaded_files)
        files_product, files_7d, files_14d, files_inv, files_age = [], [], [], [], []
        unrecognized_files = []
        
        for f in unique_files:
            fname = f.name.lower()
            if "7天流量" in fname or "traffic_7d" in fname: files_7d.append(f)
            elif "14天流量" in fname or "traffic_14d" in fname: files_14d.append(f)
            elif "库龄" in fname or "age" in fname: files_age.append(f)
            elif "海外仓" in fname or "库存" in fname or "inventory" in fname: files_inv.append(f)
            elif "产品表现" in fname or "白名单" in fname or "重要" in fname or "product" in fname: files_product.append(f)
            else: unrecognized_files.append(f.name)
        
        if unrecognized_files:
            st.warning(f"⚠️ 发现未按 SOP 规范命名的文件，系统已自动拒收并忽略：\n {', '.join(unrecognized_files)}")

        st.success("✅ 合规文件已成功读取并分类：")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("产品表现表", f"{len(files_product)} 份")
        c2.metric("7天流量表", f"{len(files_7d)} 份")
        c3.metric("14天流量表", f"{len(files_14d)} 份")
        c4.metric("库存表", f"{len(files_inv)} 份")
        c5.metric("库龄表", f"{len(files_age)} 份")
        st.markdown("---")

        if not files_product:
            st.error("❌ 严重错误：未能识别到合规的【产品表现表】！请确保文件名包含“产品表现”或“白名单”字样。")
            st.stop()

        with st.spinner("🧠 正在执行方案A(主次店铺智能分配)进行精准运算，请稍候..."):
            df_whitelist = None
            all_data_dfs = []
            for f in files_product:
                df = read_uploaded_file(f)
                df = clean_columns(df)
                df = df.drop_duplicates()
                
                if "重要" in f.name or "白名单" in f.name:
                    col_imp_msku = find_col_fuzzy_priority(df, ['MSKU', '商家SKU', 'sku'])
                    if col_imp_msku:
                        df['MSKU'] = df[col_imp_msku].apply(clean_msku_strict)
                        col_shop = find_col_fuzzy_priority(df, ['店铺', 'Shop', 'Store'])
                        if col_shop:
                            df['店铺'] = df[col_shop].astype(str).str.strip()
                            df_whitelist = df[['MSKU', '店铺']].drop_duplicates()
                        else:
                            df_whitelist = df[['MSKU']].drop_duplicates()
                    continue
                
                col_msku = find_col_fuzzy_priority(df, ['MSKU', '商家SKU'])
                col_sku = find_col_exact(df, 'SKU') 
                if not col_sku: col_sku = find_col_fuzzy_priority(df, ['FNSKU'])
                if not col_sku: col_sku = col_msku
                
                if col_msku:
                    df['MSKU'] = df[col_msku].apply(clean_msku_strict)
                    if col_sku: df['SKU_KEY'] = df[col_sku].apply(clean_msku_strict)
                    else: df['SKU_KEY'] = df['MSKU']
                    col_shop_data = find_col_fuzzy_priority(df, ['店铺', 'Shop', 'Store'])
                    if col_shop_data: df['店铺'] = df[col_shop_data].astype(str).str.strip()
                    all_data_dfs.append(df)
            
            if not all_data_dfs: st.error("❌ 无法在产品表现表中找到 MSKU 列！"); st.stop()
            df_master = pd.concat(all_data_dfs, ignore_index=True)
            df_master = df_master.drop_duplicates()
            
            if df_whitelist is not None:
                has_shop_whitelist = '店铺' in df_whitelist.columns
                has_shop_master = '店铺' in df_master.columns
                if has_shop_whitelist and has_shop_master:
                    temp = pd.merge(df_master, df_whitelist, on='MSKU', how='inner', suffixes=('', '_wl'))
                    valid_indices = []
                    for idx, row in temp.iterrows():
                        m_shop = str(row['店铺']).upper().replace(" ", "")
                        w_shop = str(row['店铺_wl']).upper().replace(" ", "")
                        if (w_shop in m_shop) or (m_shop in w_shop): valid_indices.append(idx)
                    df_master = temp.loc[valid_indices].copy()
                    if '店铺_wl' in df_master.columns: del df_master['店铺_wl']
                else:
                    df_master = pd.merge(df_master, df_whitelist[['MSKU']].drop_duplicates(), on='MSKU', how='inner')
            if df_master.empty: st.error("❌ 白名单过滤后没有剩余数据！"); st.stop()

            df_7 = process_traffic_data(files_7d, "7天")
            df_14 = process_traffic_data(files_14d, "14天")
            df_inventory = process_inventory_data(files_inv)
            df_age = process_age_data(files_age)

            def merge_traffic_with_shop_validation(master_df, traffic_df):
                if traffic_df is None or traffic_df.empty: return master_df
                if isinstance(master_df, pd.Series): master_df = master_df.to_frame()
                if isinstance(traffic_df, pd.Series): traffic_df = traffic_df.to_frame()
                master_df = master_df.loc[:, ~master_df.columns.duplicated()]
                traffic_df = traffic_df.loc[:, ~traffic_df.columns.duplicated()]
                temp = pd.merge(master_df, traffic_df, left_on='MSKU', right_on='join_key', how='left', suffixes=('', '_tr'))
                if '店铺' in temp.columns and 'traffic_shop' in temp.columns:
                    def is_match(row):
                        m_shop = str(row['店铺']).upper().replace(" ", "")
                        t_shop = str(row['traffic_shop']).upper().replace(" ", "")
                        if t_shop == 'UNKNOWN' or t_shop == 'NAN': return True 
                        return (t_shop in m_shop) or (m_shop in t_shop)
                    traffic_cols = [c for c in traffic_df.columns if c not in ['join_key', 'traffic_shop']]
                    for idx, row in temp.iterrows():
                        if not is_match(row):
                            for col in traffic_cols:
                                if col in temp.columns: temp.at[idx, col] = 0
                if 'join_key' in temp.columns: del temp['join_key']
                if 'traffic_shop' in temp.columns: del temp['traffic_shop']
                group_keys = [c for c in master_df.columns]
                traffic_cols = [c for c in traffic_df.columns if c not in ['join_key', 'traffic_shop']]
                temp = temp.groupby(group_keys, dropna=False)[traffic_cols].sum().reset_index()
                return temp

            merged = df_master.copy()
            merged = merge_traffic_with_shop_validation(merged, df_7)
            merged = merge_traffic_with_shop_validation(merged, df_14)
            merged = merged.loc[:, ~merged.columns.duplicated()]

            agg_dict = {}
            for col in merged.columns:
                if col == 'MSKU': continue
                elif col == '店铺':
                    agg_dict[col] = lambda x: ' | '.join(sorted(set(str(v).strip() for v in x.dropna() if str(v).strip() not in ['', 'nan', 'NaN', 'None'])))
                elif pd.api.types.is_numeric_dtype(merged[col]):
                    agg_dict[col] = 'sum'
                else:
                    agg_dict[col] = 'first'
                    
            merged = merged.groupby('MSKU', as_index=False, dropna=False).agg(agg_dict)

            if df_inventory is not None and not df_inventory.empty:
                df_inventory = df_inventory.loc[:, ~df_inventory.columns.duplicated()]
                merged = pd.merge(merged, df_inventory, left_on='SKU_KEY', right_on='join_key', how='left')
                if 'join_key' in merged.columns: del merged['join_key']
                
            merged = merged.loc[:, ~merged.columns.duplicated()]
            if df_age is not None and not df_age.empty:
                df_age = df_age.loc[:, ~df_age.columns.duplicated()]
                merged = pd.merge(merged, df_age, left_on='SKU_KEY', right_on='join_key', how='left')
                if 'join_key' in merged.columns: del merged['join_key']

            fill_keywords = ['7天', '14天', '21天', '28天', '库龄', '可用量', '待发货', '广告', '花费', 'CTR', 'ACOS', 'ACoAS', '点击', '曝光', '展示']
            cols_fill = [c for c in merged.columns if any(x in c for x in fill_keywords)]
            merged[cols_fill] = merged[cols_fill].fillna(0)

            col_impressions = find_col_fuzzy_priority(merged, ['展示', '广告曝光', '曝光', 'Impressions'])
            if col_impressions: merged.rename(columns={col_impressions: '广告曝光量'}, inplace=True)
            else: merged['广告曝光量'] = 0

            col_clicks = find_col_fuzzy_priority(merged, ['广告点击数', '点击'])
            if col_clicks and col_clicks != '广告点击数': merged.rename(columns={col_clicks: '广告点击数'}, inplace=True)
            elif not col_clicks: merged['广告点击数'] = 0

            col_ad_orders = find_col_fuzzy_priority(merged, ['广告订单量', '广告订单', 'Ad Orders'])
            if col_ad_orders and col_ad_orders != '广告订单': merged.rename(columns={col_ad_orders: '广告订单'}, inplace=True)
            elif not col_ad_orders: merged['广告订单'] = 0

            col_spend = find_col_fuzzy_priority(merged, ['广告花费', '花费'])
            if col_spend and col_spend != '广告花费': merged.rename(columns={col_spend: '广告花费'}, inplace=True)
            elif not col_spend: merged['广告花费'] = 0

            col_ad_sales = find_col_fuzzy_priority(merged, ['广告销售额'])
            if not col_ad_sales: merged['广告销售额'] = 0

            cols_to_numeric = ['订单毛利率', '订单毛利润', '广告花费', 'CTR', '广告CVR', 'CPC', 'ACOS', 'ACoAS', '广告点击数', '广告销售额', '广告订单', '7天销售额', '14天销售额', '广告曝光量']
            for col in cols_to_numeric:
                if col in merged.columns:
                    merged[col] = merged[col].apply(lambda x: clean_percentage_or_money(x, col))

            if '广告花费' in merged.columns and '广告销售额' in merged.columns:
                merged['ACOS'] = merged.apply(lambda x: x['广告花费'] / x['广告销售额'] if x['广告销售额'] > 0 else 0, axis=1)
            if '广告花费' in merged.columns and '广告点击数' in merged.columns:
                merged['CPC'] = merged.apply(lambda x: x['广告花费'] / x['广告点击数'] if x['广告点击数'] > 0 else 0, axis=1)
            if '广告花费' in merged.columns and '7天销售额' in merged.columns:
                merged['ACoAS'] = merged.apply(lambda x: x['广告花费'] / x['7天销售额'] if x['7天销售额'] > 0 else 0, axis=1)
            if '广告订单' in merged.columns and '广告点击数' in merged.columns:
                merged['广告CVR'] = merged.apply(lambda x: x['广告订单'] / x['广告点击数'] if x['广告点击数'] > 0 else 0, axis=1)
            if '广告点击数' in merged.columns and '广告曝光量' in merged.columns:
                merged['CTR'] = merged.apply(lambda x: x['广告点击数'] / x['广告曝光量'] if x['广告曝光量'] > 0 else 0, axis=1)

            if 'MSKU' in merged.columns:
                merged['商品属性'] = merged['MSKU'].apply(lambda x: '二手商品' if 'amzn.gr' in str(x).lower() else '')

            merged['7天日均订单'] = merged['7天订单商品总数'] / 7
            merged['14天日均订单'] = merged['14天订单商品总数'] / 14
            merged['7天销售转化率'] = merged.apply(lambda x: x['7天订单商品总数'] / x['7天会话数'] if x['7天会话数'] > 0 else 0, axis=1)
            merged['14天销售转化率'] = merged.apply(lambda x: x['14天订单商品总数'] / x['14天会话数'] if x['14天会话数'] > 0 else 0, axis=1)

            inv_cols = ['待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达']
            valid_inv = [c for c in inv_cols if c in merged.columns]
            merged['待到合计'] = merged[valid_inv].sum(axis=1)

            merged['预测日销量'] = (merged['7天日均订单'] + merged['14天日均订单']) / 2
            sku_agg = merged.groupby('MSKU').agg({'预测日销量': 'sum'}).rename(columns={'预测日销量': 'SKU_总日均'})
            merged = pd.merge(merged, sku_agg, on='MSKU', how='left')
            
            merged['理论需求量'] = merged['SKU_总日均'] * TARGET_DAYS_TOTAL
            merged['总供给'] = merged['可用量'] + merged['待到合计']
            merged['建议补货量'] = merged.apply(lambda x: max(0, x['理论需求量'] - x['总供给']), axis=1)
            merged['预计可售天数'] = merged.apply(lambda x: x['总供给'] / x['SKU_总日均'] if x['SKU_总日均'] > 0.1 else 999, axis=1)

            merged = merged.sort_values(by=['MSKU', '7天销售额'], ascending=[True, False]).reset_index(drop=True)
            
            is_duplicate = merged.duplicated(subset=['MSKU'], keep='first')
            cols_to_clear = ['可用量', '待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达', 
                             '待到合计', '总供给', '建议补货量', '0~30库龄', '31~60库龄', '61~90库龄', 
                             '91~180库龄', '181~270库龄', '271~330库龄', '331~365库龄', '365以上库龄', '181以上库龄']
            valid_cols_clear = [c for c in cols_to_clear if c in merged.columns]
            
            for col in valid_cols_clear:
                merged.loc[is_duplicate, col] = 0
            merged.loc[is_duplicate, '预计可售天数'] = None

            merged['理论需求量'] = merged['预测日销量'] * TARGET_DAYS_TOTAL
            merged.drop(columns=['SKU_总日均'], inplace=True)

            cols_to_move_front = ['预测日销量', '建议补货量', '预计可售天数', '理论需求量', '总供给']
            cols_to_move_front = [c for c in cols_to_move_front if c in merged.columns]
            cols_others = [c for c in merged.columns if c not in cols_to_move_front]
            if 'SKU_KEY' in cols_others: cols_others.remove('SKU_KEY')

            cols = cols_others[:12] + cols_to_move_front + cols_others[12:]
            def move_col(columns_list, col_to_move, ref_col, position='before'):
                if col_to_move in columns_list and ref_col in columns_list:
                    columns_list.remove(col_to_move)
                    ref_idx = columns_list.index(ref_col)
                    if position == 'before': columns_list.insert(ref_idx, col_to_move)
                    else: columns_list.insert(ref_idx + 1, col_to_move)
                return columns_list

            cols = move_col(cols, '店铺', 'MSKU', 'before')
            cols = move_col(cols, '7天日均订单', '7天销售额', 'before')
            cols = move_col(cols, '7天销售转化率', '7天销售额', 'after')
            cols = move_col(cols, '14天日均订单', '14天销售额', 'before')
            cols = move_col(cols, '14天销售转化率', '14天销售额', 'after')
            cols = move_col(cols, '待到合计', '7天内送达', 'before')
            cols = move_col(cols, '待发货', '可用量', 'before')
            cols = move_col(cols, '广告点击数', '广告花费', 'before')
            cols = move_col(cols, '广告销售额', '广告花费', 'after')
            if '广告曝光量' in cols: cols = move_col(cols, '广告曝光量', '广告点击数', 'before')
            if '广告订单' in cols: cols = move_col(cols, '广告订单', '广告销售额', 'before')
            if '181以上库龄' in cols and '商品属性' in cols: cols = move_col(cols, '商品属性', '181以上库龄', 'after')
            elif '365以上库龄' in cols and '商品属性' in cols: cols = move_col(cols, '商品属性', '365以上库龄', 'after')
            elif '商品属性' in cols: cols = move_col(cols, '商品属性', 'ASIN', 'after')

            merged = merged[cols]

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                merged.to_excel(writer, index=False, sheet_name='补货数据')
                ws = writer.sheets['补货数据']
                
                ws.insert_rows(1, amount=2)
                ws.freeze_panes = 'J4'
                ws['A1'] = "总计求和"
                ws['A2'] = "筛选求和"

                font_global = Font(name='Arial', size=11)
                font_header = Font(name='Arial', size=11, bold=True)
                align_body = Alignment(vertical='center')
                align_header = Alignment(horizontal='center', vertical='center', wrap_text=True)
                border_all = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

                color_red = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")     
                color_yellow = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")  
                color_blue = PatternFill(start_color="BDD7EE", end_color="BDD7EE", fill_type="solid")    
                color_green = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")   

                idx_to_name = {}
                for cell in ws[3]:
                    if cell.value:
                        idx_to_name[cell.col_idx] = str(cell.value).strip()

                list_header_yellow = ['店铺', '商品属性', 'ASIN', 'SKU', '父ASIN', 'SPU', '一级分类', '二级分类', '三级分类', '型号']
                list_full_blue = ['订单毛利润', '订单毛利率']
                list_header_green = ['广告点击数', '广告曝光量', '广告订单', '广告花费', '广告销售额', 'CTR', '广告CVR', 'CPC', 'ACOS', 'ACoAS', 
                                     '7天会话数', '7天页面浏览量', '7天订单商品总数', '7天日均订单', '7天销售额', '7天销售转化率', 
                                     '14天会话数', '14天页面浏览量', '14天订单商品总数', '14天日均订单', '14天销售额', '14天销售转化率']
                list_full_yellow = ['待到合计', '可用量', '待发货']

                max_row = ws.max_row
                max_col = ws.max_column

                idx_stock_days = None
                for k, v in idx_to_name.items():
                    if v == '预计可售天数':
                        idx_stock_days = k
                        break

                for row in ws.iter_rows(min_row=1, max_row=max_row, min_col=1, max_col=max_col):
                    is_stockout_risk = False
                    if row[0].row >= 4 and idx_stock_days and idx_stock_days <= len(row):
                        val_days = row[idx_stock_days - 1].value
                        if isinstance(val_days, (int, float)) and val_days < ALERT_STOCKOUT_DAYS:
                            is_stockout_risk = True

                    for cell in row:
                        r = cell.row
                        c = cell.col_idx
                        val = cell.value
                        col_name = idx_to_name.get(c, "")

                        if r <= 3:
                            cell.font = font_header
                            if r == 3: cell.alignment = align_header
                            else: cell.alignment = align_body
                        else:
                            cell.font = font_global
                            cell.alignment = align_body
                        
                        if val is not None or r == 3 or c <= 9: 
                            cell.border = border_all

                        if isinstance(val, (int, float)):
                            if any(x in col_name for x in ["率", "CTR", "ACOS", "ACoAS", "CVR", "占比"]):
                                cell.number_format = '0.00%'
                            elif any(x in col_name for x in ["送达", "待到", "可用", "库龄", "补货", "供给", "需求", "数量", "待发货", "点击", "曝光", "展示", "会话", "浏览"]):
                                cell.number_format = '#,##0'
                            elif "天数" in col_name:
                                cell.number_format = '0.0'
                            else:
                                cell.number_format = '#,##0.00'

                        if r == 3: 
                            if col_name in list_header_yellow or col_name in list_full_yellow: cell.fill = color_yellow
                            elif col_name in list_full_blue: cell.fill = color_blue
                            elif col_name in list_header_green or any(x in col_name for x in ['点击', '曝光', '展示']): cell.fill = color_green
                        
                        elif r >= 4: 
                            if col_name in list_full_blue: cell.fill = color_blue
                            elif col_name in list_full_yellow: cell.fill = color_yellow
                            
                            if is_stockout_risk and col_name in ['MSKU', 'ASIN', '可用量', '建议补货量', '预计可售天数']:
                                if isinstance(val, (int, float)) and val > 0: cell.fill = color_red
                                elif isinstance(val, str): cell.fill = color_red
                            
                            if isinstance(val, (int, float)) and val > 0:
                                if any(x in col_name for x in ['91~180库龄', '181~270库龄', '271~330库龄', '331~365库龄', '365以上库龄', '181以上库龄']):
                                    cell.fill = color_red
                                elif col_name == '61~90库龄':
                                    cell.fill = color_yellow

                ws.row_dimensions[3].height = 40
                
                for col_idx in range(1, max_col + 1):
                    col_letter = get_column_letter(col_idx)
                    max_len = 0
                    for row_idx in range(3, min(max_row, 300) + 1):
                        cell_val = ws.cell(row=row_idx, column=col_idx).value
                        if cell_val is not None:
                            val_str = str(cell_val)
                            length = sum(2.0 if '\u4e00' <= char <= '\u9fa5' else 1.1 for char in val_str)
                            if length > max_len: max_len = length
                    
                    adjusted_width = max_len + 3 
                    if adjusted_width < 10: adjusted_width = 10
                    if adjusted_width > 35: adjusted_width = 35 
                    
                    if col_idx == 1: adjusted_width = 15 
                    elif col_idx == 2: adjusted_width = 25 
                    
                    ws.column_dimensions[col_letter].width = adjusted_width

                col_map_rev = {idx_to_name[k]: get_column_letter(k) for k in idx_to_name}
                exclude_sum = ['店铺', 'MSKU', 'ASIN', 'SKU', '图片', 'image', '转化率', '可售天数', '商品属性', '分类', '型号']
                must_sum = ['销量', '销售额', '数量', '会话', '浏览', '送达', '待到', '可用', '库龄', '补货', '供给', '需求', '利润', '广告', '花费', '订单', '待发货', '点击', '曝光']

                for col_idx in range(1, max_col + 1):
                    col_letter = get_column_letter(col_idx)
                    col_name = idx_to_name.get(col_idx, "")
                    
                    is_summable = False
                    if any(ex in col_name for ex in exclude_sum): is_summable = False
                    elif any(k in col_name for k in must_sum): is_summable = True
                    
                    if col_idx >= 5 and is_summable:
                        range_str = f"{col_letter}4:{col_letter}{max_row}"
                        ws[f'{col_letter}1'] = f"=SUM({range_str})"
                        ws[f'{col_letter}2'] = f"=SUBTOTAL(109, {range_str})"
                        ws[f'{col_letter}1'].font = font_header
                        ws[f'{col_letter}2'].font = font_header
                        ws[f'{col_letter}1'].border = border_all
                        ws[f'{col_letter}2'].border = border_all
                        
                        fmt = '#,##0.00'
                        if any(x in col_name for x in ["送达", "待到", "可用", "库龄", "补货", "供给", "需求", "数量", "待发货", "点击", "曝光", "展示", "会话", "浏览"]):
                            fmt = '#,##0'
                        ws[f'{col_letter}1'].number_format = fmt
                        ws[f'{col_letter}2'].number_format = fmt

                def write_weighted_formula(name_keyword, numerator_col, denominator_col, fmt='0.00%'):
                    for col_idx, col_name in idx_to_name.items():
                        if name_keyword.upper() in col_name.upper():
                            c_num = col_map_rev.get(numerator_col)
                            c_denom = col_map_rev.get(denominator_col)
                            if not c_num or not c_denom:
                                for k, v in col_map_rev.items():
                                    if numerator_col in k: c_num = v
                                    if denominator_col in k: c_denom = v
                            if c_num and c_denom:
                                col_letter = get_column_letter(col_idx)
                                ws[f'{col_letter}1'] = f"=IFERROR(SUM({c_num}4:{c_num}{max_row})/SUM({c_denom}4:{c_denom}{max_row}),0)"
                                ws[f'{col_letter}2'] = f"=IFERROR(SUBTOTAL(109,{c_num}4:{c_num}{max_row})/SUBTOTAL(109,{c_denom}4:{c_denom}{max_row}),0)"
                                ws[f'{col_letter}1'].number_format = fmt
                                ws[f'{col_letter}2'].number_format = fmt
                                ws[f'{col_letter}1'].font = font_header
                                ws[f'{col_letter}2'].font = font_header
                                ws[f'{col_letter}1'].border = border_all
                                ws[f'{col_letter}2'].border = border_all

                write_weighted_formula('订单毛利率', '订单毛利润', '7天销售额')
                write_weighted_formula('ACoAS', '广告花费', '7天销售额', fmt='0.00%')
                write_weighted_formula('ACOS', '广告花费', '广告销售额')
                write_weighted_formula('CPC', '广告花费', '广告点击数', fmt='#,##0.00')
                write_weighted_formula('广告CVR', '广告订单', '广告点击数')
                write_weighted_formula('7天销售转化率', '7天订单商品总数', '7天会话数')
                write_weighted_formula('14天销售转化率', '14天订单商品总数', '14天会话数')
                write_weighted_formula('CTR', '广告点击数', '广告曝光量')

        st.session_state.processed_excel = output.getvalue()
        st.session_state.df_vis = merged

# ================= 5. 📊 交互式数据大屏展示 =================
if "df_vis" in st.session_state:
    df_vis = st.session_state.df_vis
    df_vis = df_vis.loc[:, ~df_vis.columns.duplicated()].copy()
    
    st.markdown("---")
    
    # 🌟 核心优化：排版升级为四列，加入店铺级联
    col_t1, col_t2, col_t3, col_t4 = st.columns([0.8, 1.2, 1.2, 1.2])
    with col_t1:
        st.markdown("##### ⏱️ 分析周期")
        time_window = st.radio("选择数据周期：", ["7天数据表现", "14天数据表现"], horizontal=True, label_visibility="collapsed")
        prefix = "7天" if "7天" in time_window else "14天"
    
    with col_t2:
        st.markdown("##### 🏢 店铺筛选器")
        if '店铺' in df_vis.columns:
            all_stores = set()
            for s in df_vis['店铺'].dropna():
                for part in str(s).split('|'):
                    cl = part.strip()
                    if cl: all_stores.add(cl)
            store_list = sorted(list(all_stores))
            selected_stores = st.multiselect("选择店铺 (支持多选)：", options=store_list, default=[], label_visibility="collapsed")
        else:
            selected_stores = []
            st.warning("⚠️ 未找到店铺列")

    # 店铺过滤逻辑
    if selected_stores:
        def has_selected_store(row_store_str):
            for s in selected_stores:
                if s in str(row_store_str): return True
            return False
        temp_df_store = df_vis[df_vis['店铺'].apply(has_selected_store)]
    else:
        temp_df_store = df_vis

    spu_col = find_col_fuzzy_priority(temp_df_store, ['SPU', '父ASIN'])
    with col_t3:
        st.markdown("##### 🔍 链接 (SPU)")
        if spu_col:
            spu_list = sorted(temp_df_store[temp_df_store[spu_col].astype(str).str.strip() != ''][spu_col].dropna().unique().tolist())
            selected_spus = st.multiselect("选择 SPU (支持多选)：", options=spu_list, default=[], label_visibility="collapsed")
        else:
            selected_spus = []
            st.warning("⚠️ 未找到 SPU 列")

    temp_df_spu = temp_df_store[temp_df_store[spu_col].isin(selected_spus)] if selected_spus and spu_col else temp_df_store
    
    with col_t4:
        st.markdown("##### 🔍 子体 (SKU)")
        if 'MSKU' in temp_df_spu.columns:
            sku_list = sorted(temp_df_spu[temp_df_spu['MSKU'].astype(str).str.strip() != '']['MSKU'].dropna().unique().tolist())
            selected_skus = st.multiselect("选择 SKU (支持多选)：", options=sku_list, default=[], label_visibility="collapsed")
        else:
            selected_skus = []
            st.warning("⚠️ 未找到 MSKU 列")

    target_df = temp_df_spu[temp_df_spu['MSKU'].isin(selected_skus)] if selected_skus else temp_df_spu
    
    is_filtered = bool(selected_stores or selected_spus or selected_skus)
    
    def safe_sum(df_source, col_name):
        if col_name in df_source.columns:
            return float(pd.to_numeric(df_source[col_name], errors='coerce').sum())
        return 0.0

    def calc_kpis(df_source):
        sales = safe_sum(df_source, f'{prefix}销售额')
        profit = safe_sum(df_source, '订单毛利润')
        ad_spend = safe_sum(df_source, '广告花费')
        supply = safe_sum(df_source, '总供给')
        daily = safe_sum(df_source, '预测日销量')
        
        margin = profit / safe_sum(df_source, '7天销售额') if safe_sum(df_source, '7天销售额') > 0 else 0.0
        acoas = ad_spend / sales if sales > 0 else 0.0
        ad_profit_ratio = ad_spend / profit if profit > 0 else 0.0
        stock_days = supply / daily if daily > 0 else 0.0 
        return profit, margin, acoas, ad_profit_ratio, sales, stock_days

    g_profit, g_margin, g_acoas, g_ad_profit_ratio, g_sales, g_stock_days = calc_kpis(df_vis)
    t_profit, t_margin, t_acoas, t_ad_profit_ratio, t_sales, t_stock_days = calc_kpis(target_df)
    
    st.markdown("##### 💡 核心 KPI 指标")
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    
    def display_metric(col, label, val_str, global_val_str=None, ratio=None):
        if is_filtered:
            if ratio is not None: col.metric(label, val_str, f"占大盘 {ratio:.1%}", delta_color="off")
            else: col.metric(label, val_str, f"大盘均值 {global_val_str}", delta_color="off")
        else:
            col.metric(label, val_str)

    display_metric(c1, "💰 1.毛利额", f"${t_profit:,.2f}", ratio=t_profit/g_profit if g_profit else 0)
    display_metric(c2, "📈 2.毛利率", f"{t_margin:.2%}", global_val_str=f"{g_margin:.2%}")
    display_metric(c3, "🔥 3.广告费占比(费比)", f"{t_acoas:.2%}", global_val_str=f"{g_acoas:.2%}")
    display_metric(c4, "⚖️ 4.广告占毛利比", f"{t_ad_profit_ratio:.2%}", global_val_str=f"{g_ad_profit_ratio:.2%}")
    display_metric(c5, f"🛒 5.销售额({prefix})", f"${t_sales:,.2f}", ratio=t_sales/g_sales if g_sales else 0)
    display_metric(c6, "📦 6.可售总天数", f"{t_stock_days:,.1f}天", global_val_str=f"{g_stock_days:,.1f}天")
    
    tab1, tab2 = st.tabs([f"🔗 聚合表现 ({prefix})", f"📦 风险明细 ({prefix})"])
    
    with tab1:
        if not spu_col:
            st.warning("⚠️ 数据表中未找到 SPU 列，无法进行聚合分析。")
        else:
            format_dict = {
                f'{prefix}销售额': '${:,.2f}', '订单毛利润': '${:,.2f}', '广告花费': '${:,.2f}', '广告销售额': '${:,.2f}', 'CPC': '${:,.2f}',
                '订单毛利率': '{:.2%}', 'ACOS': '{:.2%}', 'ACoAS': '{:.2%}', '广告CVR': '{:.2%}', 'CTR': '{:.2%}', f'{prefix}销售转化率': '{:.2%}',
                '可用量': '{:,.0f}', '待到合计': '{:,.0f}', '建议补货量': '{:,.0f}', f'{prefix}日均订单': '{:,.2f}', '预计可售天数': '{:,.1f}'
            }
            
            def _safe_div(a, b): return float(a) / float(b) if float(b) > 0 else 0.0

            if is_filtered:
                st.markdown("##### 📝 已筛选维度的明细表 (微观视角)")
                sku_df = target_df.copy()
                show_cols = ['MSKU', spu_col, f'{prefix}销售额', '订单毛利润', '订单毛利率', '广告花费', '广告销售额', 'ACOS', 'ACoAS', 'CPC', '广告CVR', 'CTR', f'{prefix}销售转化率', f'{prefix}日均订单', '可用量', '待到合计', '预计可售天数', '建议补货量']
                show_cols = [c for c in show_cols if c in sku_df.columns]
                
                total_row = pd.Series(index=show_cols, dtype=object)
                total_row['MSKU'] = '🌟 已选汇总合计'
                if spu_col in show_cols: total_row[spu_col] = '-'
                
                sum_cols_total = [f'{prefix}销售额', '订单毛利润', '广告花费', '广告销售额', '可用量', '待到合计', '建议补货量', f'{prefix}日均订单']
                for c in sum_cols_total:
                    if c in show_cols: total_row[c] = pd.to_numeric(sku_df[c], errors='coerce').sum()
                
                def get_t_sum(c): return safe_sum(target_df, c)
                if '订单毛利率' in show_cols: total_row['订单毛利率'] = _safe_div(get_t_sum('订单毛利润'), get_t_sum('7天销售额'))
                if 'ACOS' in show_cols: total_row['ACOS'] = _safe_div(get_t_sum('广告花费'), get_t_sum('广告销售额'))
                if 'ACoAS' in show_cols: total_row['ACoAS'] = _safe_div(get_t_sum('广告花费'), get_t_sum(f'{prefix}销售额'))
                if 'CPC' in show_cols: total_row['CPC'] = _safe_div(get_t_sum('广告花费'), get_t_sum('广告点击数'))
                if '广告CVR' in show_cols: total_row['广告CVR'] = _safe_div(get_t_sum('广告订单'), get_t_sum('广告点击数'))
                if 'CTR' in show_cols: total_row['CTR'] = _safe_div(get_t_sum('广告点击数'), get_t_sum('广告曝光量'))
                if f'{prefix}销售转化率' in show_cols: total_row[f'{prefix}销售转化率'] = _safe_div(get_t_sum(f'{prefix}订单商品总数'), get_t_sum(f'{prefix}会话数'))
                if '预计可售天数' in show_cols: total_row['预计可售天数'] = _safe_div(get_t_sum('总供给'), get_t_sum('预测日销量'))

                display_df = pd.concat([total_row.to_frame().T, sku_df[show_cols]], ignore_index=True)
                st.dataframe(display_df.style.format(format_dict), height=280, use_container_width=True)
                
                with st.expander("📊 点击展开: 子 SKU 销售额与毛利率矩阵图", expanded=False):
                    matrix_df = sku_df[(sku_df[f'{prefix}销售额'] > 0) | (sku_df['广告花费'] > 0)].copy()
                    if not matrix_df.empty:
                        matrix_df['bubble_size'] = pd.to_numeric(matrix_df.get(f'{prefix}日均订单', 0), errors='coerce').apply(lambda x: max(float(x), 1.0))
                        matrix_df['订单毛利率'] = pd.to_numeric(matrix_df.get('订单毛利率', 0), errors='coerce')
                        fig_sku = px.scatter(matrix_df, x=f'{prefix}销售额', y='订单毛利率', 
                                             size='bubble_size', color='ACOS',
                                             hover_name='MSKU', size_max=40, color_continuous_scale='RdYlBu_r',
                                             title="子 SKU 销售额 vs 毛利率分布")
                        
                        avg_margin_sku = pd.to_numeric(matrix_df['订单毛利率'], errors='coerce').mean()
                        avg_sales_sku = pd.to_numeric(matrix_df[f'{prefix}销售额'], errors='coerce').mean()
                        if not pd.isna(avg_margin_sku): fig_sku.add_hline(y=avg_margin_sku, line_dash="dot", annotation_text="平均毛利率")
                        if not pd.isna(avg_sales_sku): fig_sku.add_vline(x=avg_sales_sku, line_dash="dot", annotation_text=f"平均{prefix}销售额")
                        st.plotly_chart(fig_sku, use_container_width=True)

            else:
                st.markdown("##### 📝 链接维度详细数据概览 (上帝视角)")
                sum_cols = [f'{prefix}销售额', '订单毛利润', '可用量', '待到合计', '建议补货量', 
                            '广告花费', '广告销售额', '广告订单', '广告点击数', '广告曝光量', 
                            f'{prefix}订单商品总数', f'{prefix}会话数', f'{prefix}日均订单', '7天销售额', '总供给', '预测日销量']
                
                raw_sum_cols = []
                for c in sum_cols:
                    if c not in raw_sum_cols: raw_sum_cols.append(c)
                
                for c in raw_sum_cols:
                    if c not in df_vis.columns: df_vis[c] = 0.0
                    df_vis[c] = pd.to_numeric(df_vis[c], errors='coerce').fillna(0)
                
                spu_df = df_vis.groupby(spu_col, dropna=False)[raw_sum_cols].sum().reset_index()
                spu_df = spu_df[spu_df[spu_col].astype(str).str.strip() != '']
                
                spu_df['订单毛利率'] = spu_df.apply(lambda x: _safe_div(x['订单毛利润'], x['7天销售额']), axis=1)
                spu_df['ACOS'] = spu_df.apply(lambda x: _safe_div(x['广告花费'], x['广告销售额']), axis=1)
                spu_df['ACoAS'] = spu_df.apply(lambda x: _safe_div(x['广告花费'], x[f'{prefix}销售额']), axis=1)
                spu_df['CPC'] = spu_df.apply(lambda x: _safe_div(x['广告花费'], x['广告点击数']), axis=1)
                spu_df['广告CVR'] = spu_df.apply(lambda x: _safe_div(x['广告订单'], x['广告点击数']), axis=1)
                spu_df['CTR'] = spu_df.apply(lambda x: _safe_div(x['广告点击数'], x['广告曝光量']), axis=1)
                spu_df[f'{prefix}销售转化率'] = spu_df.apply(lambda x: _safe_div(x[f'{prefix}订单商品总数'], x[f'{prefix}会话数']), axis=1)
                spu_df['可售天数'] = spu_df.apply(lambda x: _safe_div(x['总供给'], x['预测日销量']), axis=1)
                
                show_cols = [spu_col, f'{prefix}销售额', '订单毛利润', '订单毛利率', '广告花费', '广告销售额', 'ACOS', 'ACoAS', 'CPC', '广告CVR', 'CTR', f'{prefix}销售转化率', f'{prefix}日均订单', '可用量', '待到合计', '可售天数', '建议补货量']
                show_cols = [c for c in show_cols if c in spu_df.columns]
                
                st.dataframe(spu_df[show_cols].style.format(format_dict), height=280, use_container_width=True)

                with st.expander("📊 点击展开: 链接 (SPU) 销售额与毛利率矩阵图", expanded=False):
                    matrix_spu = spu_df[(spu_df[f'{prefix}销售额'] > 0) | (spu_df['广告花费'] > 0)].copy()
                    if not matrix_spu.empty:
                        matrix_spu['bubble_size'] = matrix_spu[f'{prefix}日均订单'].apply(lambda x: max(float(x), 1.0))
                        fig_spu = px.scatter(matrix_spu, x=f'{prefix}销售额', y='订单毛利率', 
                                             size='bubble_size', color='ACOS',
                                             hover_name=spu_col, size_max=50, color_continuous_scale='RdYlBu_r',
                                             title=f"同链接 (SPU) 销售额 vs 毛利率分布")
                        
                        avg_margin_spu = pd.to_numeric(matrix_spu['订单毛利率'], errors='coerce').mean()
                        avg_sales_spu = pd.to_numeric(matrix_spu[f'{prefix}销售额'], errors='coerce').mean()
                        if not pd.isna(avg_margin_spu): fig_spu.add_hline(y=avg_margin_spu, line_dash="dot", annotation_text="平均毛利率")
                        if not pd.isna(avg_sales_spu): fig_spu.add_vline(x=avg_sales_spu, line_dash="dot", annotation_text=f"平均{prefix}销售额")
                        st.plotly_chart(fig_spu, use_container_width=True)

    with tab2:
        st.markdown("### 🚨 风险明细面板")
        col_chart1, col_chart2 = st.columns(2)
        with col_chart1:
            st.markdown(f"#### 🚨 紧急断货预警 TOP 15 {'(当前筛选)' if is_filtered else '(全盘)'}")
            if '预计可售天数' in target_df.columns:
                risk_df = target_df[(pd.to_numeric(target_df['预计可售天数'], errors='coerce') < ALERT_STOCKOUT_DAYS) & 
                                 (pd.to_numeric(target_df['预测日销量'], errors='coerce') > 0.1)]
                risk_df = risk_df.nsmallest(15, '预计可售天数').sort_values('预计可售天数', ascending=True)
                if not risk_df.empty:
                    fig1 = px.bar(risk_df, x='预计可售天数', y='MSKU', orientation='h',
                                  color='建议补货量', color_continuous_scale='Reds', text_auto='.1f')
                    fig1.update_layout(yaxis={'categoryorder':'total descending'}, xaxis_title="剩余可售天数")
                    st.plotly_chart(fig1, use_container_width=True)
                else:
                    st.success("非常健康！当前范围内没有产品面临断货风险。")

        with col_chart2:
            st.markdown(f"#### 🎯 当前视图 SKU 销售额与毛利分布")
            if f'{prefix}销售额' in target_df.columns and '订单毛利率' in target_df.columns:
                matrix_df_all_sku = target_df[(pd.to_numeric(target_df[f'{prefix}销售额'], errors='coerce') > 0)].copy()
                if not matrix_df_all_sku.empty:
                    matrix_df_all_sku[f'{prefix}日均订单'] = pd.to_numeric(matrix_df_all_sku.get(f'{prefix}日均订单', 0), errors='coerce').fillna(1)
                    matrix_df_all_sku['订单毛利率'] = pd.to_numeric(matrix_df_all_sku.get('订单毛利率', 0), errors='coerce')
                    
                    fig2 = px.scatter(matrix_df_all_sku, x=f'{prefix}销售额', y='订单毛利率', 
                                      size=f'{prefix}日均订单', color='ACOS',
                                      hover_name='MSKU', size_max=40, color_continuous_scale='RdYlBu_r')
                    
                    avg_margin_skus = pd.to_numeric(matrix_df_all_sku['订单毛利率'], errors='coerce').mean()
                    avg_sales_skus = pd.to_numeric(matrix_df_all_sku[f'{prefix}销售额'], errors='coerce').mean()
                    if not pd.isna(avg_margin_skus): fig2.add_hline(y=avg_margin_skus, line_dash="dot", annotation_text="平均毛利率")
                    if not pd.isna(avg_sales_skus): fig2.add_vline(x=avg_sales_skus, line_dash="dot", annotation_text=f"平均{prefix}销售额")
                    st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M')
    st.download_button(
        label="📥 下载完整【V29·三级智能穿透大盘.xlsx】",
        data=st.session_state.processed_excel,
        file_name=f"V29_多店聚合补货分析_{timestamp_str}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary"
    )
