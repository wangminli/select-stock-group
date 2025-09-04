import pandas as pd
import numpy as np
from bisect import bisect_right

# 财务因子列保持不变
fin_cols = []


def add_factor(df: pd.DataFrame, param=None,  ** kwargs) -> (pd.DataFrame, dict):
    col_name = kwargs['col_name']

    # 保持原参数不变
    fast_period = 12
    slow_period = 26
    signal_period = 9
    window = 5
    lookback = 100
    threshold = 0.1

    # 计算MACD指标
    df['ema_fast'] = df['收盘价'].ewm(span=fast_period, adjust=False).mean()
    df['ema_slow'] = df['收盘价'].ewm(span=slow_period, adjust=False).mean()
    df['macd'] = df['ema_fast'] - df['ema_slow']
    df['signal'] = df['macd'].ewm(span=signal_period, adjust=False).mean()
    df['histogram'] = df['macd'] - df['signal']

    # 初始化背离强度列
    df[col_name] = 0.0
    df['confirmed_low'] = False  # 新增：已确认的低点标志

    # 确保数据足够长
    if len(df) < lookback + window * 2:
        agg_rules = {col_name: 'last'}
        return df[[col_name]], agg_rules

    # ===== 关键修改1：历史数据验证低点 =====
    # 使用历史数据检测潜在低点（无未来函数）
    df['potential_low'] = False
    for i in range(window, len(df)):
        # 仅使用历史数据验证低点
        is_low = True
        # 检查左侧窗口（历史数据）
        for j in range(1, window + 1):
            if i - j < 0 or df['收盘价'].iloc[i] > df['收盘价'].iloc[i - j]:
                is_low = False
                break

        # 记录潜在低点（即使右侧数据不足）
        if is_low:
            df.loc[df.index[i], 'potential_low'] = True

    # ===== 关键修改2：延迟确认低点 =====
    # 当足够时间过去后确认低点
    for i in range(window * 2, len(df)):
        # 检查window天前的潜在低点是否被确认为有效低点
        check_index = i - window
        if df.loc[df.index[check_index], 'potential_low']:
            # 验证右侧窗口（使用当前已知数据）
            is_confirmed = True
            for j in range(1, window + 1):
                if i - j < 0 or df.loc[df.index[check_index], '收盘价'] > df.loc[df.index[check_index + j], '收盘价']:
                    is_confirmed = False
                    break

            if is_confirmed:
                df.loc[df.index[check_index], 'confirmed_low'] = True

    # 找出已确认的低点索引
    price_lows = df[df['confirmed_low']].index.tolist()
    price_lows = sorted(price_lows)

    # 预先筛选出所有负的MACD柱状图点
    negative_hist_indices = df[df['histogram'] < 0].index.tolist()

    # 为了快速查找，创建一个索引到位置的映射
    index_to_pos = {idx: pos for pos, idx in enumerate(df.index)}

    # ===== 关键修改3：保持文件一的信号时间点 =====
    for i in range(1, len(price_lows)):
        current_low = price_lows[i]
        prev_low = price_lows[i - 1]

        # 确保当前价格低点比前一个更低
        if df.loc[current_low, '收盘价'] >= df.loc[prev_low, '收盘价']:
            continue

        # 找到与当前价格低点时间最接近的MACD低点
        closest_histogram_low = None
        current_hist_idx = bisect_right(negative_hist_indices, current_low) - 1
        if current_hist_idx >= 0:
            closest_histogram_low = negative_hist_indices[current_hist_idx]

        # 找到与前一个价格低点时间最接近的MACD低点
        prev_closest_histogram_low = None
        prev_hist_idx = bisect_right(negative_hist_indices, prev_low) - 1
        if prev_hist_idx >= 0:
            prev_closest_histogram_low = negative_hist_indices[prev_hist_idx]

        # 检查是否满足底背离条件
        if (closest_histogram_low is not None and
                prev_closest_histogram_low is not None and
                df.loc[closest_histogram_low, 'histogram'] > df.loc[prev_closest_histogram_low, 'histogram']):

            # 计算背离强度
            price_diff = df.loc[prev_low, '收盘价'] - df.loc[current_low, '收盘价']
            histogram_diff = df.loc[closest_histogram_low, 'histogram'] - df.loc[
                prev_closest_histogram_low, 'histogram']

            # 归一化背离强度
            if price_diff > 0:
                strength = histogram_diff / price_diff
                strength = max(min(strength / threshold, 1.0), 0.0)

                # 标记在原始低点时间（但实际信号确认会延迟）
                df.loc[current_low, col_name] = strength

    # ===== 关键修改4：信号延迟处理 =====
    # 将信号延迟到确认点（消除未来函数）
    for i in range(len(df)):
        if df.loc[df.index[i], 'confirmed_low']:
            # 找到对应的潜在低点位置
            signal_time = df.index[i]

            # 将信号值复制到确认时间点
            if i + window < len(df):
                df.loc[df.index[i + window], col_name] = df.loc[signal_time, col_name]

    # 清理中间计算列
    df.drop([
        'ema_fast', 'ema_slow', 'macd', 'signal', 'histogram',
        'potential_low', 'confirmed_low'
    ], axis=1, errors='ignore', inplace=True)

    # 定义因子聚合方式
    agg_rules = {
        col_name: 'last'
    }

    return df[[col_name]], agg_rules