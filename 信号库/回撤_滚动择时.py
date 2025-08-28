"""     volatile_回撤择时因子
        * 计算方式：在给定的观察周期（lookback）内，计算当前净值相对于该窗口内最高净值的回撤（drawdown = (窗口最高净值 - 当前净值) / 窗口最高净值），这个回撤是正值。
        * 信号：当回撤大于阈值（比如0.15）时，空仓；否则满仓。
        * 信号处理：前向填充，保证信号持续性。
        震荡市/温和回调 → 滚动回撤更优（避免频繁交易）
        """


import pandas as pd

def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    # 参数设置
    lookback = int(args[0]) if len(args) > 0 else 20
    dd_threshold = float(args[1]) if len(args) > 1 else 0.15
  
    # 计算滚动最大回撤
    roll_max = equity_df['净值'].rolling(lookback, min_periods=1).max()
    drawdown = (roll_max - equity_df['净值']) / roll_max
  
    # 生成信号
    signals = pd.Series(1, index=equity_df.index)  # 默认全仓
    signals[drawdown > dd_threshold] = 0      # 回撤较大时空仓
  
    # ===== 持续持仓：将前一日信号延续到当前（信号填充）
    signals = signals.ffill().fillna(1)  # 默认开
  
    return signals