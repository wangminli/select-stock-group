import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    基于止损的择时信号（只有满仓或空仓两种状态）
    当资金曲线在近期跌幅超过阈值时，空仓
    其他情况，满仓

    :param equity_df: 资金曲线的DF
    :param args: 其他参数
        args[0]: 止损观察周期
        args[1]: 止损阈值
    :return: 返回包含 leverage 的数据
    """

    # ===== 获取策略参数
    stop_loss_period = int(args[0])
    stop_loss_threshold = float(args[1])

    # ===== 计算近期涨跌幅
    short_ret = equity_df["净值"].pct_change(stop_loss_period)

    # 计算信号
    # 当跌幅超过阈值时，空仓(0)；否则，满仓(1)
    signals = pd.Series(1.0, index=equity_df.index)
    signals[short_ret < stop_loss_threshold] = 0.0

    # print("止损信号：", signals.describe())

    return signals
