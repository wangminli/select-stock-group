"""跟踪的是从近期高点回落的幅度，计算滚动窗口内的最高点回撤（当前 vs 过去N天最高点）
 * 计算方式：在给定的观察周期（stop_loss_period）内，计算当前净值相对于该窗口内最高净值的回撤（即：drawdown = (当前净值 - 窗口最高净值) / 窗口最高净值）。注意，这个回撤是负值。
        * 信号：当回撤小于等于止损阈值（一个负值，比如-0.1）时，即下跌幅度超过阈值（比如10%）时，空仓。否则满仓。
        * 特点：它实际上是一种“回撤止损”，但触发条件是当回撤幅度（下跌幅度）超过阈值。注意，这里使用的是滚动窗口内的最高点，不是全局最高点。
        * 信号处理：对原始信号进行了3日移动平均并四舍五入取整，以平滑信号。
        熊市/高波动环境 → 滚动止损更优（快速响应极端下跌）
"""

import pandas as pd


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    基于止损的择时信号（只有满仓或空仓两种状态）
    当资金曲线在近期跌幅超过阈值时，空仓
    其他情况，满仓

    :param equity_df: 资金曲线的DF
    :param args: 其他参数
        args[0]: 止损观察周期
        args[1]: 止损阈值（负数，如-0.1表示下跌10%时止损）
    :return: 返回包含 leverage 的数据
    """

    # ===== 获取策略参数
    stop_loss_period = max(1, int(args[0]))
    stop_loss_threshold = float(args[1])  # 应为负数

    # 计算回撤：当前净值相对于过去stop_loss_period天内最高净值的回撤
    rolling_max = equity_df["净值"].rolling(stop_loss_period, min_periods=1).max()
    drawdown = (equity_df["净值"] - rolling_max) / rolling_max

    # 生成原始信号
    signals = pd.Series(1.0, index=equity_df.index)  # 默认满仓
    # 当回撤率小于等于止损阈值（即下跌幅度超过阈值）时，空仓
    signals[drawdown <= stop_loss_threshold] = 0.0

    # 信号平滑处理（3日移动平均后四舍五入）
    # 保持信号为0或1的整数值
    signals = signals.rolling(3, min_periods=1).mean().round().astype(int)

    return signals