import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    一粟_动态回撤择时.py
    核心逻辑：基于资金曲线的动态回撤深度进行择时
    本质是控制最大回撤深度。 长期稳健策略（如绝对收益）
    目标为严控回撤   	预防深度回撤
    在滚动窗口内计算回撤，并且设置两个阈值：一个用于触发空仓（回撤过大），一个用于恢复满仓（回撤恢复）。
    引入恢复阈值，避免频繁切换，但实现上使用了状态机，且恢复条件可能过于严格（需要回撤缩小到恢复阈值以下）
    基于动态回撤的择时信号（只有满仓或空仓两种状态）
    当资金曲线回撤超过阈值时，空仓
    当资金曲线回撤恢复正常时，满仓
    :param equity_df: 资金曲线DataFrame
    :param args: [回撤窗口, 回撤阈值, 恢复阈值]
    :return: 仓位信号 (0或1)
    """
    # 参数解析
    if len(args) == 1 and isinstance(args[0], tuple):
        # 处理传入单个元组的情况
        params = args[0]
    else:
        # 处理多个参数或单个数值的情况
        params = args

    # 根据参数长度动态处理
    if len(params) == 1:
        # 单参数模式：只使用窗口大小
        dd_window = int(params[0]) #回撤窗口
        dd_threshold = 0.15  # 默认回撤阈值
        recovery_threshold = 0.08  # 默认恢复阈值
    elif len(params) == 3:
        # 三参数模式
        dd_window = int(params[0])
        dd_threshold = float(params[1])
        recovery_threshold = float(params[2])
    else:
        raise ValueError(f"不支持的参数数量: {len(params)}。需要1或3个参数")

    df = equity_df.copy()
    df['净值'] = df['净值'].astype(float)

    # 计算最高净值
    df['rolling_high'] = df['净值'].rolling(window=dd_window, min_periods=1).max()

    # 计算回撤
    df['drawdown'] = (df['rolling_high'] - df['净值']) / df['rolling_high']

    # 初始化信号
    df['signal'] = 1.0
    current_state = 0  # 0=满仓, 1=空仓

    for i in range(1, len(df)):
        current_dd = df['drawdown'].iloc[i]

        if current_state == 0:  # 满仓状态
            if current_dd > dd_threshold:
                current_state = 1
                df['signal'].iloc[i] = 0.0

        else:  # 空仓状态
            df['signal'].iloc[i] = 0.0
            if current_dd < recovery_threshold:
                current_state = 0
                df['signal'].iloc[i] = 1.0

    return df['signal']