'''
来源：https://bbs.quantclass.cn/thread/68517
'''

import pandas as pd
import os
import matplotlib.pyplot as plt
from datetime import datetime
from tqdm import tqdm
import multiprocessing as mp
from functools import partial
import numpy as np

# 设置matplotlib中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Heiti TC']
plt.rcParams['axes.unicode_minus'] = False


def process_single_file(args):
    """
    处理单个股票数据文件

    参数:
    args: 包含(filepath, start_date, end_date)的元组

    返回:
    字典，包含该文件的成交额数据
    """
    filepath, start_date, end_date = args
    filename = os.path.basename(filepath)

    try:
        df = pd.read_csv(filepath, skiprows=1, encoding='gbk')

        if '中证2000成分股' not in df.columns:
            return {}

        df['交易日期'] = pd.to_datetime(df['交易日期'])
        df = df[df['交易日期'] >= start_date]
        if end_date is not None:
            df = df[df['交易日期'] <= end_date]

        file_result = {}

        for _, row in df.iterrows():
            date = row['交易日期']
            turnover = row['成交额'] / 100000000  # 转换为亿元
            is_csi2000 = row['中证2000成分股'] == 'Y'

            if date not in file_result:
                file_result[date] = {
                    'csi2000_turnover': 0,
                    'total_turnover': 0
                }

            file_result[date]['total_turnover'] += turnover

            if is_csi2000:
                file_result[date]['csi2000_turnover'] += turnover

        return file_result

    except Exception as e:
        print(f"\n处理文件 {filename} 时出错: {e}")
        return {}


def calculate_csi2000_turnover_ratio(data_folder, etf_file, start_date='2023-09-01', end_date=None, n_processes=None):
    """
    计算中证2000成分股每日成交额占全市场成交额占比并与ETF价格对比（并行版本）

    参数:
    data_folder: 包含所有股票CSV文件的文件夹路径
    etf_file: 中证2000ETF数据文件路径
    start_date: 起始日期字符串，格式为'YYYY-MM-DD'，默认为'2023-09-01'
    end_date: 截止日期字符串，格式为'YYYY-MM-DD'，默认为None（使用最新日期）
    n_processes: 并行进程数，默认为CPU核心数

    返回:
    一个合并后的DataFrame，包含日期、中证2000成交额(亿元)、全市场成交额(亿元)、占比和ETF收盘价
    """
    # 将起始日期和截止日期转换为datetime对象
    start_date = pd.to_datetime(start_date)
    if end_date is not None:
        end_date = pd.to_datetime(end_date)

    # 设置进程数
    if n_processes is None:
        n_processes = mp.cpu_count()

    print(f"使用 {n_processes} 个进程进行并行处理")

    # ==================== 第一部分：计算成交额占比（并行版本） ====================
    print("开始计算中证2000成交额占比...")

    # 获取所有CSV文件列表
    csv_files = [f for f in os.listdir(data_folder) if f.endswith('.csv')]
    total_files = len(csv_files)

    print(f"共发现 {total_files} 个股票数据文件")

    # 准备并行处理的参数
    file_args = [(os.path.join(data_folder, filename), start_date, end_date)
                 for filename in csv_files]

    # 使用多进程处理文件
    print("开始并行处理股票数据文件...")
    with mp.Pool(processes=n_processes) as pool:
        # 使用imap显示进度条
        file_results = list(tqdm(
            pool.imap(process_single_file, file_args),
            total=total_files,
            desc="处理股票数据",
            unit="file"
        ))

    # 合并所有文件的结果
    print("合并处理结果...")
    combined_result = {}

    for file_result in file_results:
        for date, data in file_result.items():
            if date not in combined_result:
                combined_result[date] = {
                    'csi2000_turnover': 0,
                    'total_turnover': 0
                }

            combined_result[date]['csi2000_turnover'] += data['csi2000_turnover']
            combined_result[date]['total_turnover'] += data['total_turnover']

    # 转换为最终格式
    result = {
        'date': [],
        'csi2000_turnover_亿元': [],
        'total_turnover_亿元': [],
        'ratio': []
    }

    for date, data in combined_result.items():
        result['date'].append(date)
        result['csi2000_turnover_亿元'].append(data['csi2000_turnover'])
        result['total_turnover_亿元'].append(data['total_turnover'])

        # 计算占比
        if data['total_turnover'] > 0:
            ratio = data['csi2000_turnover'] / data['total_turnover']
        else:
            ratio = 0
        result['ratio'].append(ratio)

    # 转换为DataFrame并处理
    turnover_df = pd.DataFrame(result)
    turnover_df = turnover_df.sort_values('date')
    turnover_df = turnover_df[turnover_df['date'] >= start_date]
    if end_date is not None:
        turnover_df = turnover_df[turnover_df['date'] <= end_date]

    # 格式化
    turnover_df['csi2000_turnover_亿元'] = turnover_df['csi2000_turnover_亿元'].round(
        2)
    turnover_df['total_turnover_亿元'] = turnover_df['total_turnover_亿元'].round(
        2)
    turnover_df['ratio'] = turnover_df['ratio'].round(4)

    print("\n成交额占比计算完成!")

    # ==================== 第二部分：处理ETF数据 ====================
    print("\n开始处理ETF数据...")

    try:
        # 读取ETF数据，假设包含日期和收盘价列
        etf_df = pd.read_csv(etf_file, skiprows=1, encoding='gbk')

        # 标准化列名并处理日期
        etf_df = etf_df.rename(columns={
            '交易日期': 'date',
            '收盘价': 'close',
        })

        etf_df['date'] = pd.to_datetime(etf_df['date'])
        etf_df = etf_df[['date', 'close']]
        etf_df = etf_df[etf_df['date'] >= start_date]
        if end_date is not None:
            etf_df = etf_df[etf_df['date'] <= end_date]
        etf_df = etf_df.sort_values('date')

        print(f"成功加载ETF数据，共 {len(etf_df)} 条记录")

    except Exception as e:
        print(f"加载ETF数据失败: {e}")
        return None

    # ==================== 第三部分：合并数据 ====================
    merged_df = pd.merge(turnover_df, etf_df, on='date', how='inner')

    # 标准化数据用于绘图
    merged_df['ratio_scaled'] = merged_df['ratio'] * 100  # 将占比转换为百分比
    merged_df['close_scaled'] = (
        merged_df['close'] / merged_df['close'].iloc[0]) * 100  # 将价格标准化为百分比

    print("\n数据合并完成!")

    # ==================== 第四部分：绘制图表 ====================
    print("\n开始绘制图表...")

    # 创建画布和双轴
    fig, ax1 = plt.subplots(figsize=(14, 8))
    ax2 = ax1.twinx()

    # 绘制成交额占比曲线（左轴）
    color = 'tab:blue'
    ax1.plot(merged_df['date'], merged_df['ratio_scaled'],
             label='中证2000成交额占比(%)', color=color, linewidth=2)
    ax1.set_xlabel('日期', fontsize=12)
    ax1.set_ylabel('成交额占比(%)', fontsize=12, color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.grid(True, linestyle='--', alpha=0.7)

    # 绘制ETF价格曲线（右轴）
    color = 'tab:red'
    ax2.plot(merged_df['date'], merged_df['close'],  # 使用原始价格而非标准化价格
             label='ETF收盘价', color=color, linewidth=2)
    ax2.set_ylabel('ETF收盘价', fontsize=12, color=color)
    ax2.tick_params(axis='y', labelcolor=color)

    # 添加标题和图例
    plt.title('中证2000成交额占比与ETF价格走势对比', fontsize=14, pad=20)

    # 合并图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2,
               loc='upper left', fontsize=10)

    # 调整日期显示格式
    fig.autofmt_xdate()

    # 保存图表和CSV数据到脚本所在目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 获取数据的起始和截止日期
    start_date_str = merged_df['date'].min().strftime('%Y%m%d')
    end_date_str = merged_df['date'].max().strftime('%Y%m%d')
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 保存图表
    chart_filename = f"中证2000成交额占比与ETF价格对比_{start_date_str}至{end_date_str}_{current_time}.png"
    chart_path = os.path.join(script_dir, chart_filename)
    plt.savefig(chart_path, dpi=300, bbox_inches='tight')
    print(f"\n图表已保存为: {chart_path}")
    
    # 保存CSV数据
    csv_filename = f"中证2000成交额占比数据_{start_date_str}至{end_date_str}_{current_time}.csv"
    csv_path = os.path.join(script_dir, csv_filename)
    
    # 准备输出的DataFrame，格式化日期列
    output_df = merged_df.copy()
    output_df['date'] = output_df['date'].dt.strftime('%Y-%m-%d')
    output_df = output_df.rename(columns={
        'date': '交易日期',
        'csi2000_turnover_亿元': '中证2000成交额(亿元)',
        'total_turnover_亿元': '全市场成交额(亿元)',
        'ratio': '成交额占比',
        'close': 'ETF收盘价',
        'ratio_scaled': '成交额占比(%)',
        'close_scaled': 'ETF价格标准化(%)'
    })
    
    # 保存为CSV文件，使用UTF-8编码支持中文
    output_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"CSV数据已保存为: {csv_path}")
    
    plt.show()
    
    return merged_df


# 使用示例
if __name__ == "__main__":
    # 设置多进程启动方法（Windows系统需要）
    mp.set_start_method('spawn', force=True)

    # 设置路径
    data_folder = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-trading-data-pro"  # 参考：https://bbs.quantclass.cn/thread/39599
    etf_file = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-etf-trading-data/159531.SZ.csv"  # ETF数据文件路径

    # 设置起始时间和进程数
    start_date = '2016-01-01'
    n_processes = mp.cpu_count() - 1  # 使用所有CPU核心，也可以手动设置如 n_processes = 4

    print(f"开始分析中证2000成交额占比与ETF价格关系")
    print(f"起始日期: {start_date}")
    print(f"使用进程数: {n_processes}")

    # 执行分析
    result = calculate_csi2000_turnover_ratio(
        data_folder, etf_file, start_date, None, n_processes)

    # 打印结果
    if result is not None:
        print("\n分析完成! 结果预览:")
        print(result.head())
