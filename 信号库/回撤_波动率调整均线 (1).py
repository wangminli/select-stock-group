import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    优化版回撤择时信号（双条件独立触发）

    参数说明：
    args[0]: 熔断回撤阈值 (默认0.12)
    args[1]: 波动率计算窗口 (默认12)
    args[2]: 长期均线窗口 (默认120)

    :return: 仓位信号(1:持仓, 0:空仓)
    """
    # ===== 参数解析 =====
    params = {
        'drawdown_threshold': 0.12,  # 熔断回撤阈值(正数)
        'vol_window': 12,
        'ma_window': 120,
    }

    # 覆盖默认参数
    for i, key in enumerate(params.keys()):
        if i < len(args):
            if key in ['vol_window', 'ma_window']:
                params[key] = int(args[i])
            elif key == 'drawdown_threshold':
                params[key] = float(args[i])

    # ===== 核心计算 =====
    # 1. 计算历史最高净值
    historical_max = equity_df["净值"].expanding().max()

    # 2. 计算当前回撤率
    drawdown = (historical_max - equity_df["净值"]) / historical_max

    # 3. 计算波动率调整因子
    returns = equity_df["净值"].pct_change().fillna(0)
    volatility = returns.rolling(params['vol_window'], min_periods=1).std()
    avg_volatility = volatility.mean()
    vol_factor = np.sqrt(volatility / avg_volatility)  # 平方根平滑调整幅度

    # 4. 调整回撤阈值（波动率越大，容忍度越高）
    adjusted_threshold = params['drawdown_threshold'] * vol_factor

    # 5. 计算长期均线
    ma_line = equity_df["净值"].rolling(params['ma_window'], min_periods=1).mean()

    # ===== 双条件独立触发 =====
    condition1 = drawdown >= adjusted_threshold  # 回撤超过动态阈值
    condition2 = equity_df["净值"] < ma_line  # 净值跌破长期均线

    # 生成最终信号
    signals = pd.Series(1.0, index=equity_df.index)
    signals[condition1 | condition2] = 0.0

    return signals