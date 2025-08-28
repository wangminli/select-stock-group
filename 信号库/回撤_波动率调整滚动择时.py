"""动态阈值调整机制：计算净值年化波动率（volatility）
基于波动率中位数（vol_median）动态调整阈值
调整公式：阈值 = 基础阈值 × (当前波动率 / 基准波动率)
当市场波动加大时自动提高阈值（减少空仓信号），波动减小时降低阈值（提高风控敏感度）
信号生成规则（回撤>阈值时空仓）
关键参数：vol_window：波动率计算窗口（默认20天）base_threshold：基准回撤阈值（默认0.15）min_threshold：最小允许阈值（默认0.05）max_threshold：最大允许阈值（默认0.30）
"""

import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    # ===== 参数解析 =====
    lookback = int(args[0]) if len(args) > 0 else 10  # 回撤观察窗口
    base_threshold = float(args[1]) if len(args) > 1 else 0.08  # 基准回撤阈值
    vol_window = int(args[2]) if len(args) > 2 else 20  # 波动率计算窗口
    min_threshold = float(args[3]) if len(args) > 3 else 0.03  # 动态阈值下限
    max_threshold = float(args[4]) if len(args) > 4 else 0.12  # 动态阈值上限

    # ===== 1. 计算净值波动率 =====
    returns = np.log(equity_df['净值']).diff()  # 对数收益率
    volatility = returns.rolling(vol_window).std() * np.sqrt(252)  # 年化波动率

    # ===== 2. 动态调整阈值 =====
    # 计算波动率中位数作为基准
    vol_median = volatility.expanding().median().fillna(volatility.median())

    # 动态调整公式：阈值 = 基础阈值 × (当前波动率 / 基准波动率)
    dynamic_threshold = base_threshold * (volatility / vol_median)

    # 应用阈值边界限制
    dynamic_threshold = dynamic_threshold.clip(lower=min_threshold, upper=max_threshold)
    dynamic_threshold.fillna(base_threshold, inplace=True)  # 填充初始NaN值

    # ===== 3. 计算滚动回撤 =====
    roll_max = equity_df['净值'].rolling(lookback, min_periods=1).max()
    drawdown = (roll_max - equity_df['净值']) / roll_max

    # ===== 4. 生成交易信号 =====
    signals = pd.Series(1, index=equity_df.index)  # 默认满仓
    # 当回撤超过动态阈值时设置为空仓
    signals[drawdown > dynamic_threshold] = 0

    # ===== 5. 信号前向填充 =====
    signals = signals.ffill().fillna(1)

    return signals