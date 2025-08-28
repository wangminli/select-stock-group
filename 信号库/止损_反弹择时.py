"""知明_止损反弹择时  双重条件过滤假信号
结合了止损和反弹两个条件，同时考虑两个周期和两个阈值

核心逻辑：基于固定周期的涨跌幅双重过滤信号
本质是捕捉短期暴跌与中期反弹的动量
趋势跟踪策略（如动量反转）
目标为快速响应趋势反转
捕捉暴跌后的反弹机会
"""

import pandas as pd
import numpy as np

def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """知明_止损反弹择时
    :param equity_df: 资金曲线的DF
    :param args: 其他参数
        args[0]: 止损观察周期
        args[1]: 止损阈值
        args[2]: 反弹观察周期
        args[3]: 反弹阈值
    :return: 返回包含 leverage 的数据
    """

    # ===== 获取策略参数
    
    if isinstance(args[0], tuple):
        stop_loss_period = int(args[0][0])
        stop_loss_threshold = float(args[0][1])
        rebound_period = int(args[0][2])  
        rebound_threshold = float(args[0][3])  
    else:
        stop_loss_period = int(args[0])
        stop_loss_threshold = float(args[1])
        rebound_period = int(args[2])  
        rebound_threshold = float(args[3])
    # ===== 计算近期涨跌幅
    short_ret = equity_df["净值"].pct_change(stop_loss_period)
    rebound_ret = equity_df["净值"].pct_change(rebound_period)  # 反弹收益率计算

    # ===== 计算信号
    # 初始化信号序列为NaN
    signals = pd.Series(np.nan, index=equity_df.index)

    # 触发反弹条件时满仓（优先处理空仓条件）
    signals[(rebound_ret > rebound_threshold)] = 1.0

    # 触发止损条件时空仓（覆盖冲突信号）
    signals[(short_ret < stop_loss_threshold)] = 0.0

    # 前向填充信号并设置默认值
    signals = signals.ffill().fillna(1.0)  # 初始状态设为满仓

    return signals
