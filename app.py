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

# ================= 2. 基础数据清洗函数 =================
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

def find_col_exact(df, keyword):
    for col in df.columns:
        if col.upper() == keyword.upper(): return col
    return None

def find_col_fuzzy_priority(df, keywords_list):
    for kw in keywords_list:
        for col in df.columns:
            if kw.lower() in col.lower(): return col
    return None

def find_col_by_pattern(df, pattern):
    for col in df.columns:
        if pattern in col: return col
    return None

def read_uploaded_file(file):
    if file.name.endswith('.csv'):
        try: return pd.read_csv(file, encoding='utf-8')
        except: 
            file.seek(0)
            return pd.read_csv(file, encoding='gbk')
    return pd.read_excel(file)

def move_col(columns_list, col_to_move, ref_col, position='before'):
    if col_to_move in columns_list and ref_col in columns_list:
        columns_list.remove(col_to_move)
        ref_idx = columns_list.index(ref_col)
        if position == 'before': columns_list.insert(ref_idx, col_to_move)
        else: columns_list.insert(ref_idx + 1, col_to_move)
    return columns_list

# ================= 3. 各模块数据处理引擎 =================
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

def process_inventory_files(uploaded_files):
    if not uploaded_files: return None
    all_inv_dfs = []
    today = pd.Timestamp.now().normalize()
    for file in uploaded_files:
        try:
            d = clean_columns(read_uploaded_file(file))
            col_sku = find_col_fuzzy_priority(d, ['SKU', 'sku', '产品'])
            col_qty = find_col_fuzzy_priority(d, ['海外仓在途', '在途', '发货量', '数量', 'Qty'])
            col_date = find_col_fuzzy_priority(d, ['实际-预计到货时间', '预计到货', 'XT-预计到货', '实际', '预计', '到货'])

            if not (col_sku and col_qty and col_date): continue
            d['join_key'] = d[col_sku].apply(clean_msku_strict)
            d['qty_clean'] = d[col_qty].apply(lambda x: clean_percentage_or_money(x, col_qty))
            d['date_clean'] = pd.to_datetime(d[col_date], errors='coerce')
            d['days_diff'] = (d['date_clean'] - today).dt.days
            
            d['待发货'] = d.apply(lambda x: x['qty_clean'] if pd.isna(x['date_clean']) else 0, axis=1)
            d['7天内送达'] = d.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and x['days_diff'] <= 7 else 0, axis=1)
            d['14天内送达'] = d.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and 7 < x['days_diff'] <= 14 else 0, axis=1)
            d['21天内送达'] = d.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and 14 < x['days_diff'] <= 21 else 0, axis=1)
            d['28天内送达'] = d.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and 21 < x['days_diff'] <= 28 else 0, axis=1)
            d['28天以上送达'] = d.apply(lambda x: x['qty_clean'] if pd.notna(x['date_clean']) and x['days_diff'] > 28 else 0, axis=1)
            all_inv_dfs.append(d[['join_key', '待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达']])
        except: pass
    return pd.concat(all_inv_dfs, ignore_index=True).groupby('join_key').sum().reset_index() if all_inv_dfs else None

def process_age_files(uploaded_files):
    if not uploaded_files: return None
    all_dfs = []
    targets = [("可用", "可用量"), ("0~30", "0~30库龄"), ("31~60", "31~60库龄"), ("61~90", "61~90库龄"), 
               ("91~180", "91~180库龄"), ("181~270", "181~270库龄"), ("271~330", "271~330库龄"), 
               ("331~365", "331~365库龄"), ("365以上", "365以上库龄"), ("181以上", "181以上库龄")]
    for file in uploaded_files:
        try:
            d = clean_columns(read_uploaded_file(file))
            col_sku = find_col_fuzzy_priority(d, ['SKU', 'sku', '产品'])
            if not col_sku: continue
            
            d['join_key'] = d[col_sku].apply(clean_msku_strict)
            result_df = pd.DataFrame({'join_key': d['join_key']})
            col_age_num = find_col_exact(d, '库龄') or find_col_fuzzy_priority(d, ['库龄', 'Age'])
            col_qty = find_col_fuzzy_priority(d, ['海外仓在库', '可用', '可用量', '在库数量', '数量'])
            has_old_format = find_col_by_pattern(d, "0~30") or find_col_by_pattern(d, "0-30")
            
            if col_age_num and col_qty and not has_old_format:
                d['qty_clean'] = d[col_qty].apply(lambda x: clean_percentage_or_money(x, col_qty))
                d['age_clean'] = d[col_age_num].apply(lambda x: clean_percentage_or_money(x, col_age_num) if pd.notna(x) else 0)
                result_df['可用量'] = d['qty_clean']
                result_df['0~30库龄'] = d.apply(lambda x: x['qty_clean'] if 0 <= x['age_clean'] <= 30 else 0, axis=1)
                result_df['31~60库龄'] = d.apply(lambda x: x['qty_clean'] if 30 < x['age_clean'] <= 60 else 0, axis=1)
                result_df['61~90库龄'] = d.apply(lambda x: x['qty_clean'] if 60 < x['age_clean'] <= 90 else 0, axis=1)
                result_df['91~180库龄'] = d.apply(lambda x: x['qty_clean'] if 90 < x['age_clean'] <= 180 else 0, axis=1)
                result_df['181~270库龄'] = d.apply(lambda x: x['qty_clean'] if 180 < x['age_clean'] <= 270 else 0, axis=1)
                result_df['271~330库龄'] = d.apply(lambda x: x['qty_clean'] if 270 < x['age_clean'] <= 330 else 0, axis=1)
                result_df['331~365库龄'] = d.apply(lambda x: x['qty_clean'] if 330 < x['age_clean'] <= 365 else 0, axis=1)
                result_df['365以上库龄'] = d.apply(lambda x: x['qty_clean'] if x['age_clean'] > 365 else 0, axis=1)
                result_df['181以上库龄'] = d.apply(lambda x: x['qty_clean'] if x['age_clean'] > 180 else 0, axis=1)
            else:
                for pattern, target_name in targets:
                    real_col = find_col_by_pattern(d, pattern)
                    if real_col and real_col in d.columns: 
                        result_df[target_name] = d[real_col].apply(lambda x: clean_percentage_or_money(x, target_name))
                    else: result_df[target_name] = 0
            all_dfs.append(result_df)
        except: pass
    return pd.concat(all_dfs, ignore_index=True).groupby('join_key').sum().reset_index() if all_dfs else None


# ================= 4. 主程序界面 =================
st.title("🚀 亚马逊智能补货系统 (完整全能版)")

col1, col2 = st.columns(2)
with col1:
    files_prod = st.file_uploader("📂 产品表现表 (主表/重要表)", accept_multiple_files=True)
    files_7d = st.file_uploader("📂 7天流量表", accept_multiple_files=True)
    files_14d = st.file_uploader("📂 14天流量表", accept_multiple_files=True)
with col2:
    files_inv = st.file_uploader("📦 库存表", accept_multiple_files=True)
    files_age = st.file_uploader("👴 库龄表", accept_multiple_files=True)

if st.button("✨ 生成并格式化报表", type="primary"):
    if not files_prod:
        st.error("请至少上传产品表现表！")
    else:
        with st.spinner('正在执行全量数据合并与格式优化...'):
            try:
                # ---------------- A. 基础数据收集与主表清洗 ----------------
                all_data = []
                for f in files_prod:
                    d = clean_columns(read_uploaded_file(f))
                    col_msku = find_col_fuzzy_priority(d, ['MSKU', '商家SKU'])
                    col_sku = find_col_exact(d, 'SKU') or find_col_fuzzy_priority(d, ['FNSKU']) or col_msku
                    if col_msku:
                        d['MSKU'] = d[col_msku].apply(clean_msku_strict)
                        d['SKU_KEY'] = d[col_sku].apply(clean_msku_strict) if col_sku else d['MSKU']
                        col_shop_data = find_col_fuzzy_priority(d, ['店铺', 'Shop', 'Store'])
                        if col_shop_data: d['店铺'] = d[col_shop_data].astype(str).str.strip()
                        all_data.append(d)
                
                df_master = pd.concat(all_data, ignore_index=True)

                # ---------------- B. 多源数据合并 ----------------
                df_7 = process_traffic_df(files_7d, "7天")
                df_14 = process_traffic_df(files_14d, "14天")
                df_inventory = process_inventory_files(files_inv)
                df_age = process_age_files(files_age)

                merged = df_master.copy()
                
                # 安全合并函数
                def merge_data_safe(master_df, right_df, on_key):
                    if right_df is None: return master_df
                    temp = pd.merge(master_df, right_df, left_on=on_key, right_on='join_key', how='left')
                    temp = temp.drop(columns=['join_key', 'traffic_shop'], errors='ignore')
                    return temp
                
                # 合并 7天、14天、库存、库龄
                merged = merge_data_safe(merged, df_7, 'MSKU')
                merged = merge_data_safe(merged, df_14, 'MSKU')
                merged = merge_data_safe(merged, df_inventory, 'SKU_KEY')
                merged = merge_data_safe(merged, df_age, 'SKU_KEY')

                # ---------------- C. 数据校准与计算逻辑 ----------------
                # 填充 NaN 为 0
                fill_keywords = ['7天', '14天', '21天', '28天', '库龄', '可用量', '待发货', '广告', '花费', 'CTR', 'ACOS', 'ACoAS', '点击', '曝光', '展示']
                cols_fill = [c for c in merged.columns if any(x in c for x in fill_keywords)]
                merged[cols_fill] = merged[cols_fill].fillna(0)

                # 提取数字
                cols_to_numeric = ['订单毛利率', '广告花费', 'CTR', '广告CVR', 'CPC', 'ACOS', 'ACoAS', '广告点击数', '广告销售额', '广告订单', '7天销售额', '广告曝光量']
                for col in cols_to_numeric:
                    if col in merged.columns: merged[col] = merged[col].apply(lambda x: clean_percentage_or_money(x, col))

                # 核心业务计算
                merged['7天日均订单'] = merged.get('7天订单商品总数', 0) / 7
                merged['14天日均订单'] = merged.get('14天订单商品总数', 0) / 14
                
                # 预防除数为0的报错
                merged['7天会话数_safe'] = merged.get('7天会话数', 1).replace(0, 1)
                merged['14天会话数_safe'] = merged.get('14天会话数', 1).replace(0, 1)
                merged['7天销售转化率'] = merged.get('7天订单商品总数', 0) / merged['7天会话数_safe']
                merged['14天销售转化率'] = merged.get('14天订单商品总数', 0) / merged['14天会话数_safe']
                merged.drop(columns=['7天会话数_safe', '14天会话数_safe'], inplace=True, errors='ignore')
                
                # 库存计算
                inv_cols = ['待发货', '7天内送达', '14天内送达', '21天内送达', '28天内送达', '28天以上送达']
                valid_inv = [c for c in inv_cols if c in merged.columns]
                merged['待到合计'] = merged[valid_inv].sum(axis=1) if valid_inv else 0

                # 补货建议计算
                merged['预测日销量'] = (merged['7天日均订单'] + merged['14天日均订单']) / 2
                merged['理论需求量'] = merged['预测日销量'] * TARGET_DAYS_TOTAL
                merged['总供给'] = merged.get('可用量', 0) + merged['待到合计']
                merged['建议补货量'] = merged.apply(lambda x: max(0, x['理论需求量'] - x['总供给']), axis=1)
                merged['预测日销量_safe'] = merged['预测日销量'].apply(lambda x: x if x > 0.1 else 9999) # 防止除以0
                merged['预计可售天数'] = merged['总供给'] / merged['预测日销量_safe']
                merged.drop(columns=['预测日销量_safe'], inplace=True, errors='ignore')

                # ---------------- D. 调整列的展示顺序 ----------------
                cols_to_move_front = [c for c in ['预测日销量', '建议补货量', '预计可售天数', '理论需求量', '总供给'] if c in merged.columns]
                cols_others = [c for c in merged.columns if c not in cols_to_move_front and c != 'SKU_KEY']
                cols = cols_others[:12] + cols_to_move_front + cols_others[12:]
                
                # 微调特定列的位置
                sort_rules = [
                    ('店铺','MSKU','before'), 
                    ('7天日均订单','7天销售额','before'), ('7天销售转化率','7天销售额','after'), 
                    ('14天日均订单','14天销售额','before'), ('14天销售转化率','14天销售额','after'), 
                    ('待到合计','7天内送达','before'), ('待发货','可用量','before'), 
                    ('广告点击数','广告花费','before'), ('广告销售额','广告花费','after')
                ]
                for rule in sort_rules:
                    cols = move_col(cols, rule[0], rule[1], rule[2])
                merged = merged[cols]

                # ---------------- E. 格式预处理 (保留两位小数) ----------------
                # 仅针对数字列应用保留小数点后两位
                numeric_cols = merged.select_dtypes(include=[np.number]).columns
                merged[numeric_cols] = merged[numeric_cols].round(2)

                # ---------------- F. Excel 高级格式输出 ----------------
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    merged.to_excel(writer, index=False, sheet_name='补货数据')
                    ws = writer.sheets['补货数据']
                    
                    # 预留顶部求和行
                    ws.insert_rows(1, amount=2) 
                    ws['A1'] = "总计求和"
                    ws['A2'] = "筛选求和"
                    
                    # 样式定义
                    base_alignment = Alignment(horizontal='center', vertical='center')
                    header_alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
                    header_font = Font(name='等线', size=11, bold=True)
                    
                    # 调整列宽 (A,B列通常是SKU，稍微放宽)
                    ws.column_dimensions['A'].width = 15
                    ws.column_dimensions['B'].width = 25
                    
                    # 【核心需求点】设置第 3 行行高为 80
                    ws.row_dimensions[3].height = 80
                    
                    # 遍历所有单元格应用格式
                    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=ws.max_row), 1):
                        for cell in row:
                            # 第一行、第二行 (求和行预留) 和 第三行(表头) 加粗
                            if row_idx <= 3:
                                cell.font = header_font
                            
                            # 【核心需求点】全文垂直居中，水平居中
                            if row_idx == 3:
                                # 【核心需求点】第三行自动换行
                                cell.alignment = header_alignment 
                            else:
                                cell.alignment = base_alignment
                            
                            # 【核心需求点】所有数据只保存小数点后两位
                            if isinstance(cell.value, (int, float)):
                                cell.number_format = '0.00'

                    # 冻结窗口以便于滚动
                    ws.freeze_panes = 'A4'

                output.seek(0)
                st.success("🎉 数据全量合并并完成极致排版！")
                st.download_button(
                    label="📥 下载智能补货表",
                    data=output,
                    file_name="亚马逊补货表_终极版.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    type="primary"
                )

            except Exception as e:
                st.error(f"❌ 运行过程中出现错误，请检查上传的数据格式: {str(e)}")
