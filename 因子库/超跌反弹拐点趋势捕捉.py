import pandas as pd
import numpy as np

# 财务因子列：此列表用于存储财务因子相关的列名称
fin_cols = []  # 财务因子列，配置后系统会自动加载对应的财务数据


def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    col_name = kwargs['col_name']
    n = int(param)  # 将参数转换为整数，表示回看区间

    # 确保数据按日期升序排列
    df = df.sort_index(ascending=True)

    # 计算回看区间内的最高价及其位置
    df['tmp_最高价'] = df['最高价_复权'].rolling(window=n, min_periods=n).max()
    df['tmp_最高价位置'] = df['最高价_复权'].rolling(window=n, min_periods=n).apply(
        lambda x: np.argmax(x), raw=True
    )

    # 初始化结果列
    df[col_name] = np.nan

    # 遍历计算每个交易日的因子值
    for i in range(n, len(df)):
        # 获取当前窗口的最高价位置
        high_idx = int(df.iloc[i]['tmp_最高价位置'])
        high_date_idx = i - n + 1 + high_idx

        # 检查最高价是否在窗口末尾（即没有后续下跌）
        if high_date_idx >= i:
            continue

        # 获取最高价日期后的最低价
        min_after_high = df.iloc[high_date_idx + 1:i + 1]['最低价_复权'].min()

        # 计算因子值
        factor_value = min_after_high / df.iloc[i]['tmp_最高价']

        # 检查当前收盘价是否满足反弹条件（比最低价高10%）
        current_close = df.iloc[i]['收盘价_复权']
        if current_close >= min_after_high * 1.05:
            df.at[df.index[i], col_name] = factor_value

    # 清理临时列
    df.drop(['tmp_最高价', 'tmp_最高价位置'], axis=1, inplace=True, errors='ignore')

    # 定义因子聚合方式
    agg_rules = {col_name: 'last'}

    # 返回新计算的因子列以及因子聚合方式
    return df[[col_name]], agg_rules