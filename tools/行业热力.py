'''
ldyr :https://bbs.quantclass.cn/thread/68294
'''
import pandas as pd
import numpy as np
import os
import glob
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from tqdm import tqdm

from core.market_essentials import cal_fuquan_price



# ========== 添加中文支持 ==========
plt.rcParams['font.sans-serif'] = ['Heiti TC', 'Arial Unicode MS']  # 黑体或Unicode字体
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 设置数据路径和结果输出文件夹
data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-trading-data-pro-2025-08-07"  # 参考：https://bbs.quantclass.cn/thread/39599

output_dir = "./industry_heatmap_results"
os.makedirs(output_dir, exist_ok=True)

# 获取所有CSV文件路径
all_files = glob.glob(os.path.join(data_path, "*.csv"))
print(f"找到 {len(all_files)} 个股票数据文件")

# 读取所有数据并合并
dfs = []
file_count = len(all_files)

# 添加文件处理进度条
for i, file in enumerate(tqdm(all_files, desc="处理股票文件")):
    try:
        # 读取必需字段
        df = pd.read_csv(file, encoding='gbk', skiprows=1,
                         usecols=['股票代码', '交易日期', '新版申万一级行业名称', '收盘价',
                                  '前收盘价', '开盘价', '最高价','股票名称', '最低价'])
        # 检查是否有足够的数据
        if len(df) <= 1:
            # print(f"\nWarning: {file} 数据为空，跳过处理")
            continue

        # 处理行业数据 - 剔除无行业数据
        df = df[df['新版申万一级行业名称'].notna()]

        # 再次检查是否有数据
        if len(df) <= 1:
            # print(f"\nWarning: {file} 无有效行业数据，跳过处理")
            continue

        # 按日期排序以计算移动平均
        df = df.sort_values('交易日期')
        # 求复权价
        df = cal_fuquan_price(df, fuquan_type="后复权")

        df['MA20'] = df['收盘价_复权'].rolling(window=20, min_periods=1).mean()

        df['bias_20'] = (df['收盘价_复权'] - df['MA20']) / df['MA20'] * 100

        dfs.append(df)

    except Exception as e:
        print(f"\nError reading {file}: {str(e)}")

if not dfs:
    raise ValueError("未读取到任何有效数据")

print("合并所有数据...")
all_data = pd.concat(dfs, ignore_index=True)

# 转换日期格式并排序
print("转换日期格式并排序...")
all_data['交易日期'] = pd.to_datetime(all_data['交易日期'])
all_data = all_data.sort_values('交易日期')

# 确定最近100个交易日
last_date = all_data['交易日期'].max()
start_date = last_date - timedelta(days=150)  # 适当放宽范围确保100个交易日
recent_data = all_data[all_data['交易日期'] >= start_date]

# 筛选最近100个有效交易日
print("筛选最近100个交易日...")
unique_dates = recent_data['交易日期'].unique()
unique_dates = np.sort(unique_dates)  # 使用numpy的sort函数
selected_dates = unique_dates[-100:] if len(unique_dates) >= 100 else unique_dates
filtered_data = recent_data[recent_data['交易日期'].isin(selected_dates)]
# 计算每日各行业比例
print("计算每日各行业比例...")
result = []
date_count = len(selected_dates)

for i, date in enumerate(tqdm(selected_dates, desc="处理每日数据")):
    daily_data = filtered_data[filtered_data['交易日期'] == date]

    industries = daily_data['新版申万一级行业名称'].unique()
    for industry in industries:
        group = daily_data[daily_data['新版申万一级行业名称'] == industry]
        total_count = len(group)
        positive_count = (group['bias_20'] > 0).sum()
        ratio = positive_count / total_count if total_count > 0 else np.nan

        # 计算强势股票代码和名称
        top_stock_code = None
        top_stock_name = None
        if total_count > 0:
            sorted_group = group.sort_values(by='bias_20', ascending=False)
            top_stock_code = sorted_group.iloc[0]['股票代码']
            top_stock_name = sorted_group.iloc[0]['股票名称']  # 新增

        result.append({
            '日期': date,
            '行业': industry,
            '比例': ratio,
            '强势股票代码': top_stock_code,
            '强势股票名称': top_stock_name  # 新增列
        })



result_df = pd.DataFrame(result)


# 修改3：添加输出最近交易日行业热度前5的功能
print("\n===== 最近交易日行业热度TOP5及强势股票 =====")
last_trade_date = selected_dates[-1]
last_trade_date_pd = pd.Timestamp(last_trade_date)
last_date_data = result_df[result_df['日期'] == last_trade_date]

# 按比例降序排序取前5
top_industries = last_date_data.sort_values(by='比例', ascending=False).head(5)

# 格式化输出
print(f"交易日: {last_trade_date_pd.strftime('%Y-%m-%d')}")
print("{:<10} {:<15} {:<10} {:<15} {:<20}".format(
    "排名", "行业", "热度比例", "强势股票代码", "强势股票名称"))
print("-" * 65)

for i, (idx, row) in enumerate(top_industries.iterrows(), 1):
    print("{:<12} {:<15} {:<12.2%} {:<15} {:<20}".format(
        i,
        row['行业'],
        row['比例'],
        row['强势股票代码'],
        row['强势股票名称']
    ))

# 转换数据为热力图格式
print("准备热力图数据...")
# 交换X轴和Y轴：行业作为列，日期作为行
heatmap_data = result_df.pivot_table(index='日期', columns='行业', values='比例')

# ==================== 优化日期格式显示 ====================
# 将日期索引格式化为"YYYY-MM-DD"格式
heatmap_data.index = heatmap_data.index.strftime('%Y-%m-%d')

# 反转日期顺序，使最新日期在顶部
heatmap_data = heatmap_data.iloc[::-1]

# 绘制热力图
print("绘制热力图...")
# 调整图形尺寸以适应新的轴方向
plt.figure(figsize=(16, 20))  # 增加高度以适应更多日期行
ax = sns.heatmap(
    heatmap_data,
    cmap='RdYlGn',
    annot=True,
    fmt=".0%",
    annot_kws={'size': 7},
    linewidths=0.5
)

# 将X轴标签移动到顶部
ax.xaxis.tick_top()  # 将X轴刻度标签移动到顶部[3](@ref)
ax.xaxis.set_label_position('top')  # 将X轴标签移动到顶部[3](@ref)

# 优化标签显示
plt.xticks(rotation=45, fontsize=9, ha='center')  # 旋转45度并居中[3](@ref)
plt.yticks(fontsize=8)  # Y轴为日期，字体稍小

# 设置标题和标签（交换X轴和Y轴标签）
plt.title('各行业热力图 (近100个交易日)', fontsize=16)
plt.xlabel('行业', fontsize=12)  # X轴现在是行业
plt.ylabel('交易日期', fontsize=12)  # Y轴现在是日期

# 调整布局防止标签被裁剪
plt.subplots_adjust(top=0.88)  # 增加顶部空间[3](@ref)

# 保存结果
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
heatmap_path = os.path.join(output_dir, f"industry_heatmap_{timestamp}.png")
plt.savefig(heatmap_path, bbox_inches='tight', dpi=300)
print(f"热力图已保存至: {heatmap_path}")

# 保存原始数据
csv_path = os.path.join(output_dir, f"industry_bias_data_{timestamp}.csv")
result_df.to_csv(csv_path, index=False, encoding='utf-8_sig')
print(f"统计结果已保存至: {csv_path}")
print("处理完成！")