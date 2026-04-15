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

# ================= 1. 全局配置 (无密码纯净版) =================
st.set_page_config(page_title="智能补货与数据中台", page_icon="📦", layout="wide")

st.markdown("""
    <style>
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    h1, h2, h3, h4, h5 { margin-bottom: 0.1rem !important; padding-bottom: 0.1rem !important; margin-top: 0.1rem !important; }
    div[data-testid="metric-container"] { padding: 0px; margin: 0px; }
    div[data-testid="stMetricValue"] { font-size: 1.4rem !important; font-weight: bold; }
    div[data-testid="stMetricLabel"] { font-size: 0.85rem !important; margin-bottom: -5px; }
    div[data-testid="stMetricDelta"] { font-size: 0.75rem !important; }
    .sop-box { background-color: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 5px solid #ffc107; font-size: 0.9em; color: #333;}
    </style>
""", unsafe_allow_html=True)
st.title("🚀 亚马逊智能补货与分析中台")

# ================= 2. 极速向量化引擎 & 核心函数 =================
def deduplicate_uploaded_files(files):
    seen, unique = set(), []
    for f in files:
        if f.name not in seen:
            seen.add(f.name)
            unique.append(f)
    return unique

@st.cache_data
def clean_msku_strict(val):
    return "" if pd.isna(val) else re.sub(r'\s+', '', str(val).strip())

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

def find_col_fuzzy(df, keywords):
    for kw in keywords:
        for col in df.columns:
            if kw.lower() in col.lower(): return col
    return None

def find_col_exact(df, keyword):
    for col in df.columns:
        if col.upper() == keyword.upper(): return col
    return None

def read_file(f):
    if f.name.endswith('.csv'):
        try: return pd.read_csv(f, encoding='utf-8')
        except: return pd.read_csv(f, encoding='gbk')
    return pd.read_excel(f)

# 流量表与库存表解析
def process_traffic(files, prefix):
    if not files: return None
    all_dfs = []
    for f in files:
        try:
            df = clean_columns(read_file(f).drop_duplicates())
            sku_col = find_col_fuzzy(df, ['SKU', '(Child)', '子ASIN'])
            if not sku_col: continue
            df['join_key'] = df[sku_col].apply(clean_msku_strict)
            shop_col = find_col_fuzzy(df, ['店铺', 'Shop', 'Store', 'Account', '账号'])
            df['traffic_shop'] = df[shop_col].astype(str).str.strip() if shop_col else 'Unknown'
            
            indicators = [
                (["会话", "Sessions"], ["广告", "Ad", "B2B"], "会话数"), 
                (["页面浏览", "Views"], ["广告", "Ad", "B2B"], "页面浏览量"),
                (["订单商品", "Units Ordered"], ["B2B", "广告", "Ad"], "订单商品总数"), 
                (["销售额", "Product Sales"], ["B2B", "广告", "Ad"], "销售额")
            ]
            found = {}
            for kws, excls, suffix in indicators:
                for col in df.columns:
                    if any(kw.lower() in col.lower() for kw in kws) and not any(ex.lower() in col.lower() for ex in excls):
                        df[col] = to_numeric_fast(df[col])
                        found[col] = f"{prefix}{suffix}"
                        break
            if not found: continue
            cols = ['join_key', 'traffic_shop'] + list(found.keys())
            temp = df[cols].groupby(['join_key', 'traffic_shop'], dropna=False).sum().reset_index().rename(columns=found)
            all_dfs.append(temp)
        except: pass
    if not all_dfs: return None
    combined = pd.concat(all_dfs, ignore_index=True)
    val_cols = [c for c in combined.columns if c not in ['join_key', 'traffic_shop']]
    return combined.groupby(['join_key', 'traffic_shop'], dropna=False)[val_cols].sum().reset_index()

def process_inventory(files):
    if not files: return None
    all_dfs, today = [], pd.Timestamp.now().normalize()
    for f in files:
        try:
            df = clean_columns(read_file(f).drop_duplicates())
            c_sku, c_qty, c_date = find_col_fuzzy(df, ['SKU', '产品']), find_col_fuzzy(df, ['海外仓在途', '在途', '发货量', '数量', 'Qty']), find_col_fuzzy(df, ['实际-预计到货时间', '预计到货', '实际', '预计', '到货', 'ETA'])
            if not (c_sku and c_qty and c_date): continue
            
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
        except: pass
    return pd.concat(all_dfs, ignore_index=True).groupby('join_key').sum().reset_index() if all_dfs else None

def process_age(files):
    if not files: return None
    all_dfs = []
    targets = [("可用", "可用量"), ("0~30", "0~30库龄"), ("31~60", "31~60库龄"), ("61~90", "61~90库龄"), ("91~180", "91~180库龄"), 
               ("181~270", "181~270库龄"), ("271~330", "271~330库龄"), ("331~365", "331~365库龄"), ("365以上", "365以上库龄"), ("181以上", "181以上库龄")]
    for f in files:
        try:
            df = clean_columns(read_file(f).drop_duplicates())
            c_sku = find_col_fuzzy(df, ['SKU', '产品'])
            if not c_sku: continue
            
            res = pd.DataFrame({'join_key': df[c_sku].apply(clean_msku_strict)})
            c_age, c_qty = find_col_fuzzy(df, ['库龄', 'Age']), find_col_fuzzy(df, ['海外仓在库', '可用', '可用量', '在库数量', '数量'])
            
            if c_age and c_qty and not find_col_fuzzy(df, ["0~30", "0-30"]):
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
                    rc = find_col_fuzzy(df, [pat])
                    res[t_name] = to_numeric_fast(df[rc]) if rc else 0
            all_dfs.append(res)
        except: pass
    return pd.concat(all_dfs, ignore_index=True).groupby('join_key').sum().reset_index() if all_dfs else None

# ================= 3. 侧边栏 =================
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
    run_btn = st.button("🚀 开始极速分析", type="primary", use_container_width=True)

# ================= 4. 核心运算与排版 =================
if run_btn:
    if not all_files: st.error("❌ 请上传数据表格！")
    else:
        unique_files = deduplicate_uploaded_files(all_files)
        f_prod, f_7d, f_14d, f_inv, f_age, unrec = [], [], [], [], [], []
        for f in unique_files:
            fname = f.name.lower()
            if "7天流量" in fname or "traffic_7d" in fname: f_7d.append(f)
            elif "14天流量" in fname or "traffic_14d" in fname: f_14d.append(f)
            elif "库龄" in fname or "age" in fname: f_age.append(f)
            elif "海外仓" in fname or "库存" in fname or "inventory" in fname: f_inv.append(f)
            elif "产品表现" in fname or "白名单" in fname or "重要" in fname or "product" in fname: f_prod.append(f)
            else: unrec.append(f.name)
        
        if unrec: st.warning(f"⚠️ 已忽略未按规范命名的文件：{', '.join(unrec)}")
        if not f_prod: st.error("❌ 严重错误：未识别到【产品表现表】！"); st.stop()

        with st.spinner("🧠 正在执行全息容错防弹运算引擎，请稍候..."):
            df_wl, all_data_dfs = None, []
            for f in f_prod:
                df = clean_columns(read_file(f).drop_duplicates())
                if "重要" in f.name or "白名单" in f.name:
                    c_msku = find_col_fuzzy(df, ['MSKU', '商家SKU', 'sku'])
                    if c_msku:
                        df['MSKU'] = df[c_msku].apply(clean_msku_strict)
                        c_shop = find_col_fuzzy(df, ['店铺', 'Shop', 'Store'])
                        if c_shop:
                            df['店铺'] = df[c_shop].astype(str).str.strip()
                            df_wl = df[['MSKU', '店铺']].drop_duplicates()
                        else: df_wl = df[['MSKU']].drop_duplicates()
                    continue
                
                c_msku, c_sku = find_col_fuzzy(df, ['MSKU', '商家SKU']), find_col_exact(df, 'SKU') or find_col_fuzzy(df, ['FNSKU'])
                if c_msku:
                    df['MSKU'] = df[c_msku].apply(clean_msku_strict)
                    df['SKU_KEY'] = df[c_sku].apply(clean_msku_strict) if c_sku else df['MSKU']
                    c_shop = find_col_fuzzy(df, ['店铺', 'Shop', 'Store'])
                    if c_shop: df['店铺'] = df[c_shop].astype(str).str.strip()
                    c_asin = find_col_fuzzy(df, ['ASIN', '子ASIN'])
                    if c_asin and c_asin != 'ASIN': df.rename(columns={c_asin: 'ASIN'}, inplace=True)
                    all_data_dfs.append(df)
            
            if not all_data_dfs: st.error("❌ 找不到 MSKU 列！"); st.stop()
            df_master = pd.concat(all_data_dfs, ignore_index=True).drop_duplicates()
            
            if df_wl is not None:
                if '店铺' in df_wl.columns and '店铺' in df_master.columns:
                    temp = pd.merge(df_master, df_wl, on='MSKU', how='inner', suffixes=('', '_wl'))
                    val_idx = [idx for idx, row in temp.iterrows() if (str(row['店铺_wl']).upper().replace(" ","") in str(row['店铺']).upper().replace(" ","") or str(row['店铺']).upper().replace(" ","") in str(row['店铺_wl']).upper().replace(" ",""))]
                    df_master = temp.loc[val_idx].copy().drop(columns=['店铺_wl'], errors='ignore')
                else:
                    df_master = pd.merge(df_master, df_wl[['MSKU']].drop_duplicates(), on='MSKU', how='inner')
            if df_master.empty: st.error("❌ 白名单过滤后无数据！"); st.stop()

            # 统一核心字段名并强转数字 (解决字符串合并丢失 Bug)
            core_map = {
                '7天销售额': ['7天销售额', '销售额(7天)'], '14天销售额': ['14天销售额', '销售额(14天)'],
                '7天订单商品总数': ['7天订单', '订单(7天)', '7天销量'], '14天订单商品总数': ['14天订单', '订单(14天)', '14天销量'],
                '广告花费': ['广告花费', '花费'], '广告销售额': ['广告销售额'], '广告订单': ['广告订单量', '广告订单'],
                '广告点击数': ['广告点击数', '点击'], '广告曝光量': ['展示', '广告曝光', '曝光'], '订单毛利润': ['订单毛利润', '毛利润', '毛利额']
            }
            for std, fz in core_map.items():
                found = find_col_fuzzy(df_master, fz)
                if found and found != std: df_master.rename(columns={found: std}, inplace=True)
                elif not found: df_master[std] = 0.0
                df_master[std] = to_numeric_fast(df_master[std])

            df_7, df_14 = process_traffic(f_7d, "7天"), process_traffic(f_14d, "14天")
            df_inventory, df_age = process_inventory(f_inv), process_age(f_age)

            def merge_traffic(m_df, t_df):
                if t_df is None or t_df.empty: return m_df
                m_df, t_df = m_df.loc[:, ~m_df.columns.duplicated()], t_df.loc[:, ~t_df.columns.duplicated()]
                t_cols = [c for c in t_df.columns if c not in ['join_key', 'traffic_shop']]
                # 防重血统拦截
                overlap = [c for c in t_cols if c in m_df.columns]
                if overlap:
                    t_df = t_df.drop(columns=overlap)
                    t_cols = [c for c in t_df.columns if c not in ['join_key', 'traffic_shop']]
                if not t_cols: return m_df
                
                temp = pd.merge(m_df, t_df, left_on='MSKU', right_on='join_key', how='left')
                if '店铺' in temp.columns and 'traffic_shop' in temp.columns:
                    for idx, row in temp.iterrows():
                        m_s, t_s = str(row['店铺']).upper().replace(" ", ""), str(row['traffic_shop']).upper().replace(" ", "")
                        if t_s != 'UNKNOWN' and t_s != 'NAN' and t_s not in m_s and m_s not in t_s:
                            for col in t_cols:
                                if col in temp.columns: temp.at[idx, col] = 0
                return temp.drop(columns=['join_key', 'traffic_shop'], errors='ignore').groupby([c for c in m_df.columns if c not in t_cols], dropna=False)[t_cols].sum().reset_index()

            merged = merge_traffic(merge_traffic(df_master.copy(), df_7), df_14).loc[:, ~merged.columns.duplicated()]

            # 多店铺预聚合
            agg_d = {}
            for c in merged.columns:
                if c == 'MSKU': continue
                elif c == '店铺': agg_d[c] = lambda x: ' | '.join(sorted(set(str(v).strip() for v in x.dropna() if str(v).strip() not in ['', 'nan'])))
                elif pd.api.types.is_numeric_dtype(merged[c]): agg_d[c] = 'sum'
                else: agg_d[c] = 'first'
            merged = merged.groupby('MSKU', as_index=False, dropna=False).agg(agg_d)

            # 贴入库存库龄
            for df_t in [df_inventory, df_age]:
                if df_t is not None and not df_t.empty:
                    df_t = df_t.loc[:, ~df_t.columns.duplicated()]
                    merged = pd.merge(merged, df_t, left_on='SKU_KEY', right_on='join_key', how='left').drop(columns=['join_key'], errors='ignore')
            merged = merged.loc[:, ~merged.columns.duplicated()].fillna(0)

            # 🌟 防弹容错算力区 (安全取值，应对漏传表格)
            def safe_div(n, d): return np.where(merged.get(d, 0) > 0, merged.get(n, 0) / merged.get(d, 1), 0)
            
            merged['ACOS'] = safe_div('广告花费', '广告销售额')
            merged['CPC'] = safe_div('广告花费', '广告点击数')
            merged['ACoAS'] = safe_div('广告花费', '7天销售额')
            merged['广告CVR'] = safe_div('广告订单', '广告点击数')
            merged['CTR'] = safe_div('广告点击数', '广告曝光量')
            merged['订单毛利率'] = safe_div('订单毛利润', '7天销售额')

            merged['商品属性'] = merged['MSKU'].apply(lambda x: '二手商品' if 'amzn.gr' in str(x).lower() else '')
            
            merged['7天日均订单'] = merged.get('7天订单商品总数', 0) / 7
            merged['14天日均订单'] = merged.get('14天订单商品总数', 0) / 14
            merged['7天销售转化率'] = safe_div('7天订单商品总数', '7天会话数')
            merged['14天销售转化率'] = safe_div('14天订单商品总数', '14天会话数')

            inv_c = [c for c in ['待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达'] if c in merged.columns]
            merged['待到合计'] = merged[inv_c].sum(axis=1) if inv_c else 0
            
            merged['预测日销量'] = (merged['7天日均订单'] + merged['14天日均订单']) / 2
            s_agg = merged.groupby('MSKU').agg({'预测日销量': 'sum'}).rename(columns={'预测日销量': 'SKU_总日均'})
            merged = pd.merge(merged, s_agg, on='MSKU', how='left')
            
            # 🌟 这里是之前报错的核心：加入了 .get() 防弹壳
            merged['理论需求量'] = merged['SKU_总日均'] * TARGET_DAYS_TOTAL
            merged['总供给'] = merged.get('可用量', 0) + merged.get('待到合计', 0)
            merged['建议补货量'] = np.maximum(0, merged['理论需求量'] - merged['总供给'])
            merged['预计可售天数'] = np.where(merged['SKU_总日均'] > 0.1, merged['总供给'] / merged['SKU_总日均'], 999)

            # 方案A防重拦截
            sort_c = '7天销售额' if '7天销售额' in merged.columns else 'MSKU'
            merged = merged.sort_values(by=['MSKU', sort_c], ascending=[True, False]).reset_index(drop=True)
            
            is_dup = merged.duplicated(subset=['MSKU'], keep='first')
            clr_cols = ['可用量', '待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达', '待到合计', '总供给', '建议补货量', '0~30库龄', '31~60库龄', '61~90库龄', '91~180库龄', '181~270库龄', '271~330库龄', '331~365库龄', '365以上库龄', '181以上库龄']
            for c in [c for c in clr_cols if c in merged.columns]: merged.loc[is_dup, c] = 0
            merged.loc[is_dup, '预计可售天数'] = None

            merged['理论需求量'] = merged['预测日销量'] * TARGET_DAYS_TOTAL
            merged.drop(columns=['SKU_总日均', 'SKU_KEY'], errors='ignore', inplace=True)

            # 列排序
            mv_front = [c for c in ['预测日销量', '建议补货量', '预计可售天数', '理论需求量', '总供给'] if c in merged.columns]
            others = [c for c in merged.columns if c not in mv_front]
            cols = others[:12] + mv_front + others[12:]
            
            def move_col(lst, c, r, pos='before'):
                if c in lst and r in lst:
                    lst.remove(c)
                    lst.insert(lst.index(r) + (0 if pos=='before' else 1), c)
                return lst

            cols = move_col(cols, '店铺', 'MSKU', 'before')
            if 'ASIN' in cols: cols = move_col(cols, 'ASIN', 'MSKU', 'after')
            for a, b, p in [('7天日均订单', '7天销售额', 'before'), ('7天销售转化率', '7天销售额', 'after'), ('14天日均订单', '14天销售额', 'before'), ('14天销售转化率', '14天销售额', 'after'), ('待到合计', '7天内送达', 'before'), ('待发货', '可用量', 'before'), ('广告点击数', '广告花费', 'before'), ('广告销售额', '广告花费', 'after'), ('广告曝光量', '广告点击数', 'before'), ('广告订单', '广告销售额', 'before'), ('商品属性', 'ASIN', 'after')]:
                cols = move_col(cols, a, b, p)

            merged = merged[cols]

            # Excel 渲染引擎
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                merged.to_excel(writer, index=False, sheet_name='补货数据')
                ws = writer.sheets['补货数据']
                ws.insert_rows(1, amount=2)
                ws.freeze_panes = 'J4'
                ws['A1'], ws['A2'] = "总计求和", "筛选求和"

                f_g, f_h = Font(name='Arial', size=11), Font(name='Arial', size=11, bold=True)
                a_b, a_h = Alignment(vertical='center'), Alignment(horizontal='center', vertical='center', wrap_text=True)
                bd = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

                c_r, c_y, c_b, c_g = [PatternFill(start_color=c, end_color=c, fill_type="solid") for c in ["FFC7CE", "FFEB9C", "BDD7EE", "C6EFCE"]]
                i2n = {cell.col_idx: str(cell.value).strip() for cell in ws[3] if cell.value}
                
                ly, lb, lg, lfy = ['店铺', '商品属性', 'ASIN', 'SKU', '父ASIN', 'SPU', '一级分类', '二级分类', '三级分类', '型号'], ['订单毛利润', '订单毛利率'], ['广告点击数', '广告曝光量', '广告订单', '广告花费', '广告销售额', 'CTR', '广告CVR', 'CPC', 'ACOS', 'ACoAS', '7天会话数', '7天页面浏览量', '7天订单商品总数', '7天日均订单', '7天销售额', '7天销售转化率', '14天会话数', '14天页面浏览量', '14天订单商品总数', '14天日均订单', '14天销售额', '14天销售转化率'], ['待到合计', '可用量', '待发货']
                idx_sd = next((k for k, v in i2n.items() if v == '预计可售天数'), None)

                for r in ws.iter_rows(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
                    is_risk = (r[0].row >= 4 and idx_sd and isinstance(r[idx_sd-1].value, (int, float)) and r[idx_sd-1].value < ALERT_STOCKOUT_DAYS)
                    for cell in r:
                        ro, co, val, cname = cell.row, cell.col_idx, cell.value, i2n.get(cell.col_idx, "")
                        cell.font, cell.alignment = (f_h, a_h if ro==3 else a_b) if ro <= 3 else (f_g, a_b)
                        if val is not None or ro == 3 or co <= 9: cell.border = bd
                        
                        if isinstance(val, (int, float)):
                            if any(x in cname for x in ["率", "CTR", "ACOS", "ACoAS", "CVR", "占比"]): cell.number_format = '0.00%'
                            elif any(x in cname for x in ["送达", "待到", "可用", "库龄", "补货", "供给", "需求", "数量", "待发货", "点击", "曝光", "展示", "会话", "浏览"]): cell.number_format = '#,##0'
                            elif "天数" in cname: cell.number_format = '0.0'
                            else: cell.number_format = '#,##0.00'

                        if ro == 3:
                            if cname in ly or cname in lfy: cell.fill = c_y
                            elif cname in lb: cell.fill = c_b
                            elif cname in lg or any(x in cname for x in ['点击', '曝光', '展示']): cell.fill = c_g
                        elif ro >= 4:
                            if cname in lb: cell.fill = c_b
                            elif cname in lfy: cell.fill = c_y
                            if is_risk and cname in ['MSKU', 'ASIN', '可用量', '建议补货量', '预计可售天数'] and (isinstance(val, str) or (isinstance(val, (int, float)) and val>0)): cell.fill = c_r
                            if isinstance(val, (int, float)) and val > 0:
                                if any(x in cname for x in ['91~180库龄', '181~270库龄', '271~330库龄', '331~365库龄', '365以上库龄', '181以上库龄']): cell.fill = c_r
                                elif cname == '61~90库龄': cell.fill = c_y

                ws.row_dimensions[3].height = 40
                for c in range(1, ws.max_column + 1):
                    ml = max([sum(2.0 if '\u4e00' <= char <= '\u9fa5' else 1.1 for char in str(ws.cell(r, c).value)) for r in range(3, min(ws.max_row, 300) + 1) if ws.cell(r, c).value], default=0)
                    ws.column_dimensions[get_column_letter(c)].width = min(max(ml + 3, 10), 35) if c not in [1, 2] else (15 if c == 1 else 25)

                col_map_rev = {v: get_column_letter(k) for k, v in i2n.items()}
                for c in range(5, ws.max_column + 1):
                    cn = i2n.get(c, "")
                    if any(k in cn for k in ['销量', '销售额', '数量', '会话', '浏览', '送达', '待到', '可用', '库龄', '补货', '供给', '需求', '利润', '广告', '花费', '订单', '待发货', '点击', '曝光']) and not any(ex in cn for ex in ['店铺', 'MSKU', 'ASIN', 'SKU', '图片', 'image', '转化率', '可售天数', '商品属性', '分类', '型号']):
                        cl, rs = get_column_letter(c), f"{get_column_letter(c)}4:{get_column_letter(c)}{ws.max_row}"
                        ws[f'{cl}1'], ws[f'{cl}2'] = f"=SUM({rs})", f"=SUBTOTAL(109, {rs})"
                        for ri in [1, 2]:
                            ws[f'{cl}{ri}'].font, ws[f'{cl}{ri}'].border = f_h, bd
                            ws[f'{cl}{ri}'].number_format = '#,##0' if any(x in cn for x in ["送达", "待到", "可用", "库龄", "补货", "供给", "需求", "数量", "待发货", "点击", "曝光", "展示", "会话", "浏览"]) else '#,##0.00'

                for kw, num, den, fmt in [('订单毛利率', '订单毛利润', '7天销售额', '0.00%'), ('ACoAS', '广告花费', '7天销售额', '0.00%'), ('ACOS', '广告花费', '广告销售额', '0.00%'), ('CPC', '广告花费', '广告点击数', '#,##0.00'), ('广告CVR', '广告订单', '广告点击数', '0.00%'), ('7天销售转化率', '7天订单商品总数', '7天会话数', '0.00%'), ('14天销售转化率', '14天订单商品总数', '14天会话数', '0.00%'), ('CTR', '广告点击数', '广告曝光量', '0.00%')]:
                    for c, cn in i2n.items():
                        if kw.upper() in cn.upper():
                            c_n, c_d = col_map_rev.get(num), col_map_rev.get(den)
                            if c_n and c_d:
                                cl = get_column_letter(c)
                                ws[f'{cl}1'], ws[f'{cl}2'] = f"=IFERROR(SUM({c_n}4:{c_n}{ws.max_row})/SUM({c_d}4:{c_d}{ws.max_row}),0)", f"=IFERROR(SUBTOTAL(109,{c_n}4:{c_n}{ws.max_row})/SUBTOTAL(109,{c_d}4:{c_d}{ws.max_row}),0)"
                                for ri in [1, 2]: ws[f'{cl}{ri}'].number_format, ws[f'{cl}{ri}'].font, ws[f'{cl}{ri}'].border = fmt, f_h, bd

        st.session_state.processed_excel = output.getvalue()
        st.session_state.df_vis = merged

# ================= 5. 大屏展示 =================
if "df_vis" in st.session_state:
    df_vis = st.session_state.df_vis.loc[:, ~st.session_state.df_vis.columns.duplicated()].copy()
    st.markdown("---")
    
    c_t1, c_t2, c_t3, c_t4, c_t5 = st.columns([0.8, 1, 1, 1, 1.2])
    with c_t1:
        st.markdown("##### ⏱️ 分析周期")
        prefix = "7天" if "7天" in st.radio("周期", ["7天数据表现", "14天数据表现"], horizontal=True, label_visibility="collapsed") else "14天"
    
    df_f = df_vis.copy()
    opt_s = sorted(list(set([p.strip() for s in df_f['店铺'].dropna() for p in str(s).split('|') if p.strip()]))) if '店铺' in df_f.columns else []
    spu_c = find_col_fuzzy(df_f, ['SPU', '父ASIN'])
    
    with c_t2:
        st.markdown("##### 🏢 店铺")
        sel_s = st.multiselect("店", opt_s, label_visibility="collapsed")
        if sel_s: df_f = df_f[df_f['店铺'].apply(lambda x: any(s in str(x) for s in sel_s))]
    with c_t3:
        st.markdown("##### 🔗 SPU")
        sel_p = st.multiselect("S", sorted(df_f[spu_c].dropna().unique().tolist()) if spu_c else [], label_visibility="collapsed")
        if sel_p: df_f = df_f[df_f[spu_c].isin(sel_p)]
    with c_t4:
        st.markdown("##### 🏷️ ASIN")
        sel_a = st.multiselect("A", sorted(df_f['ASIN'].dropna().unique().tolist()) if 'ASIN' in df_f.columns else [], label_visibility="collapsed")
        if sel_a: df_f = df_f[df_f['ASIN'].isin(sel_a)]
    with c_t5:
        st.markdown("##### 📦 穿透 MSKU")
        sel_m = st.multiselect("M", sorted(df_f['MSKU'].dropna().unique().tolist()) if 'MSKU' in df_f.columns else [], label_visibility="collapsed")
        if sel_m: df_f = df_f[df_f['MSKU'].isin(sel_m)]

    target_df = df_f
    is_filtered = bool(sel_s or sel_p or sel_a or sel_m)

    def s_sum(df, col): return float(pd.to_numeric(df.get(col, 0), errors='coerce').sum())
    def calc_kpi(df):
        sa, pr, ad, sp, da = s_sum(df, f'{prefix}销售额'), s_sum(df, '订单毛利润'), s_sum(df, '广告花费'), s_sum(df, '总供给'), s_sum(df, '预测日销量')
        return pr, pr/s_sum(df, '7天销售额') if s_sum(df, '7天销售额')>0 else 0, ad/sa if sa>0 else 0, ad/pr if pr>0 else 0, sa, sp/da if da>0 else 0

    g_p, g_m, g_ac, g_ap, g_s, g_sd = calc_kpi(df_vis)
    t_p, t_m, t_ac, t_ap, t_s, t_sd = calc_kpi(target_df)
    
    st.markdown("##### 💡 核心 KPI 指标")
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    def d_met(c, l, v, gv=None, r=None): c.metric(l, v, f"占大盘 {r:.1%}" if r is not None else f"大盘 {gv}", delta_color="off") if is_filtered else c.metric(l, v)
    
    d_met(m1, "💰 1.毛利额", f"${t_p:,.2f}", r=t_p/g_p if g_p else 0)
    d_met(m2, "📈 2.毛利率", f"{t_m:.2%}", gv=f"{g_m:.2%}")
    d_met(m3, "🔥 3.广告费占比", f"{t_ac:.2%}", gv=f"{g_ac:.2%}")
    d_met(m4, "⚖️ 4.广告占毛利", f"{t_ap:.2%}", gv=f"{g_ap:.2%}")
    d_met(m5, f"🛒 5.销售额({prefix})", f"${t_s:,.2f}", r=t_s/g_s if g_s else 0)
    d_met(m6, "📦 6.可售天数", f"{t_sd:,.1f}天", gv=f"{g_sd:,.1f}天")
    
    tab1, tab2 = st.tabs([f"🔗 聚合表现 ({prefix})", f"📦 风险明细 ({prefix})"])
    with tab1:
        if not spu_c: st.warning("⚠️ 未找到 SPU 列")
        else:
            fmt_d = {f'{prefix}销售额': '${:,.2f}', '订单毛利润': '${:,.2f}', '广告花费': '${:,.2f}', '广告销售额': '${:,.2f}', 'CPC': '${:,.2f}', '订单毛利率': '{:.2%}', 'ACOS': '{:.2%}', 'ACoAS': '{:.2%}', '广告CVR': '{:.2%}', 'CTR': '{:.2%}', f'{prefix}销售转化率': '{:.2%}', '可用量': '{:,.0f}', '待到合计': '{:,.0f}', '建议补货量': '{:,.0f}', f'{prefix}日均订单': '{:,.2f}', '预计可售天数': '{:,.1f}'}
            def _sd(n, d): return s_sum(target_df, n) / s_sum(target_df, d) if s_sum(target_df, d) > 0 else 0
            
            if is_filtered:
                st.markdown("##### 📝 逆向穿透子体明细表")
                s_cols = [c for c in ['MSKU', spu_c, 'ASIN', f'{prefix}销售额', '订单毛利润', '订单毛利率', '广告花费', '广告销售额', 'ACOS', 'ACoAS', 'CPC', '广告CVR', 'CTR', f'{prefix}销售转化率', f'{prefix}日均订单', '可用量', '待到合计', '预计可售天数', '建议补货量'] if c in target_df.columns]
                t_row = pd.Series(index=s_cols, dtype=object)
                t_row['MSKU'] = '🌟 已选合计'
                for c in [spu_c, 'ASIN']:
                    if c in s_cols: t_row[c] = '-'
                for c in [f'{prefix}销售额', '订单毛利润', '广告花费', '广告销售额', '可用量', '待到合计', '建议补货量', f'{prefix}日均订单']:
                    if c in s_cols: t_row[c] = pd.to_numeric(target_df.get(c, 0), errors='coerce').sum()
                
                if '订单毛利率' in s_cols: t_row['订单毛利率'] = _sd('订单毛利润', '7天销售额')
                if 'ACOS' in s_cols: t_row['ACOS'] = _sd('广告花费', '广告销售额')
                if 'ACoAS' in s_cols: t_row['ACoAS'] = _sd('广告花费', f'{prefix}销售额')
                if 'CPC' in s_cols: t_row['CPC'] = _sd('广告花费', '广告点击数')
                if '广告CVR' in s_cols: t_row['广告CVR'] = _sd('广告订单', '广告点击数')
                if 'CTR' in s_cols: t_row['CTR'] = _sd('广告点击数', '广告曝光量')
                if f'{prefix}销售转化率' in s_cols: t_row[f'{prefix}销售转化率'] = _sd(f'{prefix}订单商品总数', f'{prefix}会话数')
                if '预计可售天数' in s_cols: t_row['预计可售天数'] = _sd('总供给', '预测日销量')

                st.dataframe(pd.concat([t_row.to_frame().T, target_df[s_cols]], ignore_index=True).style.format(fmt_d), height=280, use_container_width=True)
            else:
                st.markdown("##### 📝 SPU 维度大盘概览")
                sc = [f'{prefix}销售额', '订单毛利润', '可用量', '待到合计', '建议补货量', '广告花费', '广告销售额', '广告订单', '广告点击数', '广告曝光量', f'{prefix}订单商品总数', f'{prefix}会话数', f'{prefix}日均订单', '7天销售额', '总供给', '预测日销量']
                sc = list(set([c for c in sc if c in df_vis.columns]))
                for c in sc: df_vis[c] = pd.to_numeric(df_vis[c], errors='coerce').fillna(0)
                sp_df = df_vis.groupby(spu_c, dropna=False)[sc].sum().reset_index()
                sp_df = sp_df[sp_df[spu_c].astype(str).str.strip() != '']
                
                sp_df['订单毛利率'] = np.where(sp_df.get('7天销售额',0)>0, sp_df.get('订单毛利润',0)/sp_df.get('7天销售额',1), 0)
                sp_df['ACOS'] = np.where(sp_df.get('广告销售额',0)>0, sp_df.get('广告花费',0)/sp_df.get('广告销售额',1), 0)
                sp_df['ACoAS'] = np.where(sp_df.get(f'{prefix}销售额',0)>0, sp_df.get('广告花费',0)/sp_df.get(f'{prefix}销售额',1), 0)
                sp_df['CPC'] = np.where(sp_df.get('广告点击数',0)>0, sp_df.get('广告花费',0)/sp_df.get('广告点击数',1), 0)
                sp_df['广告CVR'] = np.where(sp_df.get('广告点击数',0)>0, sp_df.get('广告订单',0)/sp_df.get('广告点击数',1), 0)
                sp_df['CTR'] = np.where(sp_df.get('广告曝光量',0)>0, sp_df.get('广告点击数',0)/sp_df.get('广告曝光量',1), 0)
                sp_df[f'{prefix}销售转化率'] = np.where(sp_df.get(f'{prefix}会话数',0)>0, sp_df.get(f'{prefix}订单商品总数',0)/sp_df.get(f'{prefix}会话数',1), 0)
                sp_df['预计可售天数'] = np.where(sp_df.get('预测日销量',0)>0, sp_df.get('总供给',0)/sp_df.get('预测日销量',1), 0)
                
                sh_c = [c for c in [spu_c, f'{prefix}销售额', '订单毛利润', '订单毛利率', '广告花费', '广告销售额', 'ACOS', 'ACoAS', 'CPC', '广告CVR', 'CTR', f'{prefix}销售转化率', f'{prefix}日均订单', '可用量', '待到合计', '预计可售天数', '建议补货量'] if c in sp_df.columns]
                st.dataframe(sp_df[sh_c].style.format(fmt_d), height=280, use_container_width=True)

            with st.expander("📊 SPU/SKU 矩阵图", expanded=False):
                m_df = (target_df if is_filtered else sp_df).copy()
                m_df = m_df[(pd.to_numeric(m_df.get(f'{prefix}销售额',0), errors='coerce') > 0) | (pd.to_numeric(m_df.get('广告花费',0), errors='coerce') > 0)]
                if not m_df.empty:
                    m_df['bs'] = pd.to_numeric(m_df.get(f'{prefix}日均订单',0), errors='coerce').clip(lower=1)
                    m_df['订单毛利率'] = pd.to_numeric(m_df.get('订单毛利率',0), errors='coerce')
                    fig = px.scatter(m_df, x=f'{prefix}销售额', y='订单毛利率', size='bs', color='ACOS' if 'ACOS' in m_df.columns else None, hover_name='MSKU' if is_filtered else spu_c, size_max=45, color_continuous_scale='RdYlBu_r')
                    am, asls = m_df['订单毛利率'].mean(), m_df[f'{prefix}销售额'].mean()
                    if not pd.isna(am): fig.add_hline(y=am, line_dash="dot", annotation_text="均毛利率")
                    if not pd.isna(asls): fig.add_vline(x=asls, line_dash="dot", annotation_text=f"均{prefix}销售额")
                    st.plotly_chart(fig, use_container_width=True)

    with tab2:
        c_c1, c_c2 = st.columns(2)
        with c_c1:
            st.markdown(f"#### 🚨 断货预警 TOP 15 {'(当前穿透)' if is_filtered else '(全盘)'}")
            if '预计可售天数' in target_df.columns:
                r_df = target_df[(pd.to_numeric(target_df['预计可售天数'], errors='coerce') < ALERT_STOCKOUT_DAYS) & (pd.to_numeric(target_df['预测日销量'], errors='coerce') > 0.1)].nsmallest(15, '预计可售天数').sort_values('预计可售天数')
                if not r_df.empty:
                    fg1 = px.bar(r_df, x='预计可售天数', y='MSKU', orientation='h', color='建议补货量', color_continuous_scale='Reds', text_auto='.1f')
                    fg1.update_layout(yaxis={'categoryorder':'total descending'}, xaxis_title="剩余天数")
                    st.plotly_chart(fg1, use_container_width=True)
                else: st.success("十分安全！无断货风险。")
        with c_c2:
            st.markdown(f"#### 🎯 SKU 矩阵微观分布")
            if f'{prefix}销售额' in target_df.columns:
                m_a = target_df[pd.to_numeric(target_df[f'{prefix}销售额'], errors='coerce') > 0].copy()
                if not m_a.empty:
                    m_a['订单毛利率'] = pd.to_numeric(m_a.get('订单毛利率',0), errors='coerce')
                    fg2 = px.scatter(m_a, x=f'{prefix}销售额', y='订单毛利率', size=pd.to_numeric(m_a.get(f'{prefix}日均订单',1), errors='coerce').clip(lower=1), color='ACOS' if 'ACOS' in m_a.columns else None, hover_name='MSKU', size_max=40, color_continuous_scale='RdYlBu_r')
                    am, asls = m_a['订单毛利率'].mean(), m_a[f'{prefix}销售额'].mean()
                    if not pd.isna(am): fg2.add_hline(y=am, line_dash="dot")
                    if not pd.isna(asls): fg2.add_vline(x=asls, line_dash="dot")
                    st.plotly_chart(fg2, use_container_width=True)

    st.markdown("---")
    st.download_button(label="📥 下载完整【V36·防弹容错护盾版.xlsx】", data=st.session_state.processed_excel, file_name=f"V36_容错防弹大盘_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")
