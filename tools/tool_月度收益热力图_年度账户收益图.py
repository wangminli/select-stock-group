"""
来自学员的分享： https://bbs.quantclass.cn/thread/67516

"""
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

# 解决中文显示问题
plt.rcParams['font.sans-serif'] = ['Heiti TC']
plt.rcParams['axes.unicode_minus'] = False  # 正常显示负号
import matplotlib.font_manager as fm


# 文件路径
monthly_file_path = r'/Users/wangminli/PycharmProjects/select-stock-group/data/回测结果/超跌反弹拐点趋势捕捉_乖离率量化因子_换手率/月度账户收益.csv'
yearly_file_path = r'/Users/wangminli/PycharmProjects/select-stock-group/data/回测结果/超跌反弹拐点趋势捕捉_乖离率量化因子_换手率/年度账户收益.csv'

try:
    # ==================== 读取和处理数据 ====================
    # 读取月度CSV文件
    df_monthly = pd.read_csv(monthly_file_path, encoding='utf-8-sig')

    # 读取年度CSV文件
    df_yearly = pd.read_csv(yearly_file_path, encoding='utf-8-sig')

    # 处理月度数据
    if df_monthly.shape[1] > 2:
        df_monthly.set_index(df_monthly.columns[0], inplace=True)
        month_columns = list(range(1, 13))
        if len(df_monthly.columns) >= 12:
            df_monthly.columns = month_columns[:len(df_monthly.columns)]
        else:
            for month in month_columns:
                if month not in df_monthly.columns:
                    df_monthly[month] = np.nan
        pivot_table = df_monthly.T
    else:
        df_monthly.columns = ['交易日期', '涨跌幅']
        if df_monthly['涨跌幅'].dtype == 'object':
            df_monthly['涨跌幅'] = df_monthly['涨跌幅'].str.replace('%', '').astype(float) / 100
        else:
            df_monthly['涨跌幅'] = df_monthly['涨跌幅'] / 100
        df_monthly['交易日期'] = pd.to_datetime(df_monthly['交易日期'])
        df_monthly['年份'] = df_monthly['交易日期'].dt.year
        df_monthly['月份'] = df_monthly['交易日期'].dt.month
        pivot_table = df_monthly.pivot_table(
            index='月份',
            columns='年份',
            values='涨跌幅',
            aggfunc='mean'
        )

    # 确保月份索引正确排序
    pivot_table = pivot_table.reindex(range(1, 13))

    # 处理年度数据
    df_yearly.columns = ['交易日期', '涨跌幅']
    if df_yearly['涨跌幅'].dtype == 'object':
        df_yearly['涨跌幅'] = df_yearly['涨跌幅'].str.replace('%', '').astype(float) / 100
    else:
        df_yearly['涨跌幅'] = df_yearly['涨跌幅'] / 100
    df_yearly['交易日期'] = pd.to_datetime(df_yearly['交易日期'])
    df_yearly['年份'] = df_yearly['交易日期'].dt.year

    # ==================== 创建并排图表 ====================
    # 创建包含两个子图的图形
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

    # 设置颜色
    positive_color = '#FF1105'  # 深红色（正收益）
    negative_color = '#2081FF'  # 深蓝色（负收益）
    line_color = '#333333'  # 深灰色（折线）

    # 左侧：月度热力图
    # cmap = LinearSegmentedColormap.from_list('custom_red_blue', heatmap_colors, N=100)
    cmap = sns.diverging_palette(240, 10, s=200, l=50 ,as_cmap=True)  # 230°是蓝色，10°接近红色

    heatmap = sns.heatmap(
        pivot_table * 100,
        annot=True,
        fmt=".2f",
        cmap=cmap,
        center=0,
        linewidths=1,
        annot_kws={'size': 9},
        cbar_kws={'label': '收益率 (%)', 'shrink': 0.8},
        ax=ax1
    )

    # 设置热力图标题和标签
    ax1.set_title('月度收益热力图（单位：%）', fontsize=16, pad=16)
    ax1.set_xlabel('年份', fontsize=12)
    ax1.set_ylabel('月份', fontsize=12)

    # 添加百分比符号到热力图标注，并对重点数值加粗
    for text in ax1.texts:
        t = float(text.get_text())
        text.set_text(f"{t:.2f}%")
        if abs(t) >= 10:
            text.set_fontweight('bold')
            text.set_color('black')

    # 右侧：年度柱状折线图
    # 为每个柱子设置颜色（正收益红色，负收益蓝色）
    colors = [positive_color if x >= 0 else negative_color for x in df_yearly['涨跌幅']]

    # 绘制柱状图
    bars = ax2.bar(df_yearly['年份'], df_yearly['涨跌幅'] * 100, color=colors, alpha=0.7)

    # 绘制折线图
    ax2_right = ax2.twinx()
    line = ax2_right.plot(df_yearly['年份'], df_yearly['涨跌幅'] * 100,
                          color=line_color, marker='o', linewidth=2, markersize=6)

    # 设置柱状图标题和标签
    ax2.set_title('年度账户收益（单位：%）', fontsize=16, pad=16)
    ax2.set_xlabel('年份', fontsize=12)
    ax2.set_ylabel('收益率 (%)', fontsize=12)
    ax2_right.set_ylabel('收益率趋势', fontsize=12, color=line_color)

    # 设置y轴范围，确保0线可见
    y_min = min(df_yearly['涨跌幅'] * 100) * 1.1
    y_max = max(df_yearly['涨跌幅'] * 100) * 1.1
    ax2.set_ylim(y_min, y_max)
    ax2_right.set_ylim(y_min, y_max)

    # 添加0线参考
    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)

    # 添加数值标签
    for i, v in enumerate(df_yearly['涨跌幅'] * 100):
        ax2.text(df_yearly['年份'].iloc[i], v + (0.02 * y_max if v >= 0 else -0.04 * y_max),
                 f'{v:.2f}%', ha='center', va='bottom' if v >= 0 else 'top', fontsize=9)

    # 设置x轴刻度
    ax2.set_xticks(df_yearly['年份'])
    ax2.set_xticklabels(df_yearly['年份'], rotation=45)

    # 添加网格
    ax2.grid(True, alpha=0.3)

    # 添加图例
    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor=positive_color, label='正收益'),
        Patch(facecolor=negative_color, label='负收益'),
        plt.Line2D([0], [0], color=line_color, marker='o', label='收益趋势')
    ]
    ax2.legend(handles=legend_elements, loc='upper left')

    # 调整布局
    plt.tight_layout()
    plt.show()

except Exception as e:
    print(f"处理数据时出错: {e}")
    import traceback

    traceback.print_exc()
    print("请检查文件路径和格式是否正确")