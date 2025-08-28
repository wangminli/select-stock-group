import pandas as pd


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    简化版回撤择时信号（双条件独立触发）

    参数说明：
    args[0]: 熔断回撤阈值 (默认0.12)
    args[1]: 长期均线窗口 (默认120)

    :return: 仓位信号(1:持仓, 0:空仓)
    """
    # ===== 参数解析 =====
    params = {
        'drawdown_threshold': 0.12,  # 熔断回撤阈值(正数)
        'ma_window': 120,
    }

    # 覆盖默认参数
    if len(args) > 0:
        params['drawdown_threshold'] = float(args[0])
    if len(args) > 1:
        params['ma_window'] = int(args[1])

    # ===== 核心计算 =====
    # 1. 计算历史最高净值
    historical_max = equity_df["净值"].expanding().max()

    # 2. 计算当前回撤率
    drawdown = (historical_max - equity_df["净值"]) / historical_max

    # 3. 计算长期均线
    ma_line = equity_df["净值"].rolling(params['ma_window'], min_periods=1).mean()

    # ===== 双条件独立触发 =====
    condition1 = drawdown >= params['drawdown_threshold']  # 回撤超过固定阈值
    condition2 = equity_df["净值"] < ma_line  # 净值跌破长期均线

    # 生成最终信号
    signals = pd.Series(1.0, index=equity_df.index)
    signals[condition1 | condition2] = 0.0

    return signals