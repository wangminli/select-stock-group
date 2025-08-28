"""
利用EXPMA（指数移动平均线）择时
"""

import pandas as pd
import numpy as np



def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    根据资金曲线，使用EXPMA进行择时
    :param equity_df: 资金曲线的DF
    :param args: 其他参数，args[0]为EXPMA周期
    :return: 返回包含 leverage 的数据
    """

    # ===== 获取策略参数
    period = int(args[0])  # EXPMA周期
    prices = equity_df["净值"]
    # 计算平滑因子 α = 2 / (period + 1)
    alpha = 2 / (period + 1)
    
    # 使用pandas的ewm方法计算EXPMA
    expma = prices.ewm(alpha=alpha, adjust=False).mean()


    # 默认空仓
    signals = pd.Series(0.0, index=equity_df.index)

    # 净值在EXPMA之上，才持有
    above = equity_df["净值"] > expma
    signals.loc[above] = 1.0

    return signals


