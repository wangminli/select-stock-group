import pandas as pd
import numpy as np
import os
import datetime as dt
from tqdm import tqdm
import seaborn as sns
import matplotlib.pyplot as plt

# 设置数据文件夹路径
data_folder = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-trading-data-pro-2025-08-07"
# 输出结果
output_path = r"/Users/wangminli/PycharmProjects/select-stock-group/data/行情_申万一级行业估值跟踪表.csv"
# 创建热力图
heatmap_path = r"/Users/wangminli/PycharmProjects/select-stock-group/data/行情_申万一级行业估值跟踪表_热力图.png"

# 设置中文字体，优先使用Microsoft YaHei，如果不可用则使用备选字体
plt.rcParams['font.family'] = ['Microsoft YaHei', 'PingFang HK', 'STHeiti', 'SimHei', 'Arial Unicode MS', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

def calculate_stock_history(data_folder):
    """收集所有股票的历史数据，用于计算行业整体指标"""
    all_stock_data = []

    # 获取所有CSV文件
    csv_files = [f for f in os.listdir(data_folder)
                 if f.endswith('.csv') and f.startswith(('sh', 'sz'))]

    print(f"开始收集 {len(csv_files)} 只股票的历史数据...")

    # 使用进度条
    for file_name in tqdm(csv_files):
        file_path = os.path.join(data_folder, file_name)
        try:
            df = pd.read_csv(file_path, skiprows=1, encoding='gbk')
        except Exception as e:
            continue

        # 检查必需的列是否存在
        required_columns = [
            '股票代码', '股票名称', '交易日期', '收盘价', '总市值',
            '净利润TTM', '净资产', '净利润(当季)', '新版申万一级行业名称'
        ]

        if not all(col in df.columns for col in required_columns):
            continue

        # 选择所需列并处理数据
        df = df[required_columns]
        df['交易日期'] = pd.to_datetime(df['交易日期'])

        # 计算个股市盈率和市净率
        df['市盈率(PE)'] = df['总市值'] / df['净利润TTM']
        df['市净率(PB)'] = df['总市值'] / df['净资产']

        # 过滤无效值
        df = df[(df['市盈率(PE)'] > 0) & (df['市盈率(PE)'] < 1000) &
                (df['市净率(PB)'] > 0) & (df['市净率(PB)'] < 100)]

        all_stock_data.append(df)

    if not all_stock_data:
        print("没有找到有效股票数据")
        return None

    # 合并所有股票数据
    all_data = pd.concat(all_stock_data, ignore_index=True)
    return all_data


def calculate_industry_percentiles(industry_data, current_pe, current_pb):
    """计算行业当前估值在历史中的百分位"""
    # 计算市盈率百分位（修正逻辑）
    if not industry_data.empty and not np.isnan(current_pe):
        pe_percentile = (industry_data['行业市盈率'] <= current_pe).mean()
    else:
        pe_percentile = np.nan

    # 计算市净率百分位
    if not industry_data.empty and not np.isnan(current_pb):
        pb_percentile = (industry_data['行业市净率'] <= current_pb).mean()
    else:
        pb_percentile = np.nan

    return pe_percentile, pb_percentile


def calculate_industry_returns(industry_history, latest_market_value, latest_date):
    """计算行业涨跌幅指标"""
    # 获取一周前的日期（5个交易日）
    one_week_ago = latest_date - pd.Timedelta(days=7)
    week_data = industry_history[industry_history['交易日期'] >= one_week_ago]

    if len(week_data) > 1:
        week_start_value = week_data.iloc[0]['总市值']
        weekly_return = (latest_market_value / week_start_value - 1) if week_start_value != 0 else np.nan
    else:
        weekly_return = np.nan

    # 获取当月开始的日期
    month_start = pd.Timestamp(year=latest_date.year, month=latest_date.month, day=1)
    month_data = industry_history[industry_history['交易日期'] >= month_start]

    if not month_data.empty:
        month_start_value = month_data.iloc[0]['总市值']
        monthly_return = (latest_market_value / month_start_value - 1) if month_start_value != 0 else np.nan
    else:
        monthly_return = np.nan

    # 获取今年开始的日期
    year_start = pd.Timestamp(year=latest_date.year, month=1, day=1)
    year_data = industry_history[industry_history['交易日期'] >= year_start]

    if not year_data.empty:
        year_start_value = year_data.iloc[0]['总市值']
        ytd_return = (latest_market_value / year_start_value - 1) if year_start_value != 0 else np.nan
    else:
        ytd_return = np.nan

    return weekly_return, monthly_return, ytd_return


def create_heatmap(data, output_path):
    """创建热力图并保存"""
    # 从数据中提取最新交易日期
    if '交易日期' in data.columns:
        # 尝试获取第一个日期（因为所有行的交易日期应该相同）
        latest_date = data['交易日期'].iloc[0]
        # 确保是Timestamp对象
        if isinstance(latest_date, str):
            latest_date = pd.to_datetime(latest_date)
        # 格式化日期为"YYYY年MM月DD日"
        date_str = latest_date.strftime('%Y年%m月%d日')
    else:
        date_str = "未知日期"

    # 选择数值型数据并转换为浮点数（处理百分比数据）
    heatmap_data = data.copy()

    # 处理百分比数据，将字符串转换为浮点数
    percent_columns = ['市盈率百分位', '市净率百分位', '近一周涨跌幅', '当月涨跌幅', '今年以来涨跌幅']
    for col in percent_columns:
        if col in heatmap_data.columns:
            heatmap_data[col] = heatmap_data[col].apply(
                lambda x: float(x.replace('%', '')) / 100 if isinstance(x, str) and '%' in x else float(
                    x) if isinstance(x, str) else x
            )

    # 选择要绘制热力图的数值列
    numeric_columns = ['市净率百分位', '市盈率百分位',
                       '近一周涨跌幅', '当月涨跌幅', '今年以来涨跌幅', '最新市净率(PB)', '最新市盈率(PE)']

    # 确保这些列存在且为数值类型
    available_columns = [col for col in numeric_columns if col in heatmap_data.columns]
    heatmap_df = heatmap_data[['申万一级行业'] + available_columns].set_index('申万一级行业')

    # 创建注释矩阵，将百分位指标格式化为百分数
    annot_matrix = heatmap_df.astype(float).copy()
    for col in annot_matrix.columns:
        if '百分位' in col:
            # 将百分位指标转换为百分数字符串格式
            annot_matrix[col] = annot_matrix[col].apply(
                lambda x: f"{x * 100:.0f}%" if not pd.isna(x) else ''
            )
        elif '涨跌幅' in col:
            # 将涨跌幅指标转换为百分数字符串格式
            annot_matrix[col] = annot_matrix[col].apply(
                lambda x: f"{x * 100:.1f}%" if not pd.isna(x) else ''
            )
        else:
            # 其他指标保持2位小数格式
            annot_matrix[col] = annot_matrix[col].apply(
                lambda x: f"{x:.2f}" if not pd.isna(x) else ''
            )

    # 创建多个颜色映射方案
    from matplotlib.colors import LinearSegmentedColormap

    # 为不同指标设置不同的中心值和颜色映射
    fig, ax = plt.subplots(figsize=(10, 10))

    # 第一组：百分位指标（中心值0.5）
    percentile_cols = ['市盈率百分位', '市净率百分位']
    percentile_mask = np.ones_like(heatmap_df.astype(float), dtype=bool)
    for i, col in enumerate(heatmap_df.columns):
        if col in percentile_cols:
            percentile_mask[:, i] = False  # 显示这些列

    # 绘制百分位指标热力图（中心值0.5）
    sns.heatmap(heatmap_df.astype(float),
                mask=percentile_mask,
                annot=annot_matrix,  # 使用格式化后的注释矩阵
                fmt='',  # 因为已经格式化，所以不需要额外格式
                cmap='RdYlGn_r',
                center=0.5,  # 中心值设为0.5
                vmin=0, vmax=1,  # 设置范围0-1
                linewidths=0.4,  # 单元格之间的线宽
                cbar=False,  # 不单独显示颜色条
                ax=ax,
                annot_kws={"weight": "bold", "size": 10})  # 添加这一行，设置注释文本加粗和大小

    # 第二组：涨跌幅指标（中心值0）
    change_cols = ['近一周涨跌幅', '当月涨跌幅', '今年以来涨跌幅']
    change_mask = np.ones_like(heatmap_df.astype(float), dtype=bool)
    for i, col in enumerate(heatmap_df.columns):
        if col in change_cols:
            change_mask[:, i] = False  # 显示这些列

    # 绘制涨跌幅指标热力图（中心值0）
    sns.heatmap(heatmap_df.astype(float),
                mask=change_mask,
                annot=annot_matrix,  # 使用格式化后的注释矩阵
                fmt='',  # 因为已经格式化，所以不需要额外格式
                cmap='RdYlGn_r',
                center=0,  # 中心值设为0
                vmin=-1, vmax=1,  # 设置范围0-1
                linewidths=0.4,  # 单元格之间的线宽
                cbar=False,  # 不单独显示颜色条
                ax=ax,
                annot_kws={"weight": "bold", "size": 10})  # 添加这一行，设置注释文本加粗和大小

    # 第三组：绝对估值指标（白色底色）
    value_cols = ['最新市盈率(PE)', '最新市净率(PB)']
    value_mask = np.ones_like(heatmap_df.astype(float), dtype=bool)
    for i, col in enumerate(heatmap_df.columns):
        if col in value_cols:
            value_mask[:, i] = False  # 显示这些列

    # 创建白色到浅灰色的颜色映射
    white_cmap = LinearSegmentedColormap.from_list('white_cmap', ['white', 'lightgray'], N=256)

    # 绘制绝对估值指标热力图（白色底色）
    sns.heatmap(heatmap_df.astype(float),
                mask=value_mask,
                annot=annot_matrix,  # 使用格式化后的注释矩阵
                fmt='',  # 因为已经格式化，所以不需要额外格式
                cmap=white_cmap,  # 使用白色底色
                center=0,  # 中心值
                linewidths=0.5,  # 单元格之间的线宽
                cbar=False,  # 不单独显示颜色条
                ax=ax,
                annot_kws={"weight": "bold", "size": 10})  # 添加这一行，设置注释文本加粗和大小


    # 调整x轴标签位置
    ax.set_xticks(np.arange(len(heatmap_df.columns)) + 0.5)
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position("top")
    ax.set_xticklabels(heatmap_df.columns, rotation=0, ha='center', fontweight='bold')
    ax.set_yticklabels(ax.get_yticklabels(), fontweight='bold')  # 添加这行来加粗Y轴标签
    ax.set_ylabel('')

    # 调整布局并保存
    plt.tight_layout()
    # 修改标题，加入日期信息
    plt.suptitle(f'申万一级行业估值跟踪表（数据截止{date_str}）', fontsize=16, y=0.95,fontweight='bold')

    # 添加说明文字（估值分位说明）
    explanation_text = (
        "估值分位说明：\n"
        "0-20%(低估) | 20-40%(正常偏低) | 40-60%(正常) | 60-80%(高估) | 80%以上(极度高估)"
    )

    # 在热力图下方添加说明文字
    plt.figtext(0.5, 0.02, explanation_text, ha='center', fontsize=11,  # y从0.05调整到0.02
                bbox=dict(boxstyle="round,pad=0.5", facecolor="lightgray", alpha=0.5))

    # 最后调整布局，预留更多底部空间
    plt.tight_layout(rect=[0, 0.06, 1, 0.97])  #

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    print(f"热力图已保存至: {output_path}")


def analyze_industries_advanced(data_folder):
    """改进的行业分析方法：基于行业整体估值历史计算百分位"""
    # 收集所有股票历史数据
    all_data = calculate_stock_history(data_folder)
    if all_data is None:
        return None

    # 获取最新交易日
    latest_date = all_data['交易日期'].max()

    # 计算每个行业在每个交易日的整体估值
    industry_daily = all_data.groupby(['新版申万一级行业名称', '交易日期']).agg({
        '总市值': 'sum',
        '净利润TTM': 'sum',
        '净资产': 'sum'
    }).reset_index()

    # 计算行业整体市盈率和市净率
    industry_daily['行业市盈率'] = industry_daily['总市值'] / industry_daily['净利润TTM']
    industry_daily['行业市净率'] = industry_daily['总市值'] / industry_daily['净资产']

    # 过滤极端值
    industry_daily = industry_daily[
        (industry_daily['行业市盈率'] > 0) & (industry_daily['行业市盈率'] < 1000) &
        (industry_daily['行业市净率'] > 0) & (industry_daily['行业市净率'] < 100)
        ]

    # 获取最新日期的行业估值
    latest_industry_data = industry_daily[industry_daily['交易日期'] == latest_date]

    # 计算每个行业的估值百分位
    industry_metrics = []

    for industry in latest_industry_data['新版申万一级行业名称'].unique():
        industry_latest = latest_industry_data[
            latest_industry_data['新版申万一级行业名称'] == industry
            ].iloc[0]

        # 获取该行业过去10年的历史数据
        ten_years_ago = latest_date - pd.DateOffset(years=10)
        industry_history = industry_daily[
            (industry_daily['新版申万一级行业名称'] == industry) &
            (industry_daily['交易日期'] >= ten_years_ago) &
            (industry_daily['交易日期'] <= latest_date)
            ]

        # 计算当前行业估值在历史中的百分位
        current_pe = industry_latest['行业市盈率']
        current_pb = industry_latest['行业市净率']

        pe_percentile, pb_percentile = calculate_industry_percentiles(
            industry_history, current_pe, current_pb
        )

        # 计算行业涨跌幅指标
        weekly_return, monthly_return, ytd_return = calculate_industry_returns(
            industry_history, industry_latest['总市值'], latest_date
        )

        # 计算行业股票数量
        industry_stocks = all_data[
            (all_data['新版申万一级行业名称'] == industry) &
            (all_data['交易日期'] == latest_date)
            ]['股票代码'].nunique()

        industry_metrics.append({
            '申万一级行业': industry,
            '最新市盈率(PE)': current_pe,
            '最新市净率(PB)': current_pb,
            '市盈率百分位': pe_percentile,
            '市净率百分位': pb_percentile,
            '近一周涨跌幅': weekly_return,
            '当月涨跌幅': monthly_return,
            '今年以来涨跌幅': ytd_return,
            '包含股票数量': industry_stocks,
            '交易日期': latest_date
        })

    # 创建行业指标DataFrame
    industry_df = pd.DataFrame(industry_metrics)
    industry_df['平均百分位'] = (industry_df['市盈率百分位'] + industry_df['市净率百分位']) / 2
    industry_df = industry_df.sort_values(by='平均百分位', ascending=False)
    industry_df = industry_df.drop('平均百分位', axis=1)  # 删除临时列，不显示在最终结果中
    # 格式化数据：将百分位数据转换为百分比格式，其他数据保留2位小数
    # 首先复制一份原始数据用于计算
    industry_df_output = industry_df.copy()

    # 处理百分位数据：乘以100并保留2位小数，添加百分号
    percent_columns = ['市盈率百分位', '市净率百分位', '近一周涨跌幅', '当月涨跌幅', '今年以来涨跌幅']
    for col in percent_columns:
        industry_df_output[col] = industry_df_output[col].apply(
            lambda x: f"{x * 100:.2f}%" if not pd.isna(x) else "N/A"
        )

    # 处理其他数值数据：保留2位小数
    float_columns = industry_df.columns.difference(['申万一级行业', '包含股票数量', '交易日期'] + percent_columns)
    for col in float_columns:
        industry_df_output[col] = industry_df_output[col].apply(
            lambda x: f"{x:.2f}" if not pd.isna(x) else "N/A"
        )

    return industry_df_output


# 执行改进的行业分析
industry_results = analyze_industries_advanced(data_folder)

if industry_results is not None:
    industry_results.to_csv(output_path, index=False, encoding='gbk')
    print(f"改进的行业分析结果已保存至: {output_path}")
    print("\n各行业指标预览:")
    print(industry_results.head(10))
    create_heatmap(industry_results, heatmap_path)

else:
    print("未能生成行业分析结果")