"""
【彭帅龙】基于小市值优化动量择时的选股策略  "动量择时", "params": [3,12,-0.07]
动量强度确认模型（关注上涨强度）进攻型策略
趋势跟随导向：核心逻辑是捕捉强动量（短期强势+长期趋势）时满仓，否则空仓。
使用双周期动量交叉 + 绝对阈值：
满仓条件 = 短期动量 > 阈值 + 长期动量 > 0
默认空仓（无单独开仓条件）
"""

import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:

    # ===== 获取策略参数
    if len(args) > 1:
        short_period = int(args[0])  # 短期动量周期
        long_period = int(args[1])  # 长期动量周期
        momentum_threshold = float(args[2])  # 动量阈值
    else:
        # ===== 寻优时，获取策略参数
        short_period = int(args[0][0])  # 短期动量周期
        long_period = int(args[0][1])  # 长期动量周期
        momentum_threshold = float(args[0][2])  # 动量阈值

    # ===== 计算动量指标
    # 计算短期和长期收益率
    short_ret = equity_df["净值"].pct_change(short_period)
    long_ret = equity_df["净值"].pct_change(long_period)

    # 默认空仓
    signals = pd.Series(0.0, index=equity_df.index)

    # 根据动量强弱调整仓位
    # 强动量：满仓
    strong_momentum = (short_ret > momentum_threshold) & (long_ret > 0)
    signals.loc[strong_momentum] = 1.0

    # 填充NaN值（前面的周期可能没有足够的数据计算动量）
    signals = signals.fillna(1.0)

    return signals
