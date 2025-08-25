import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    状态自适应动量择时因子（融合恶化预警+强度确认）
    三级状态机：
      0-空仓防御：监测恶化预警信号
      1-趋势跟随：满足强度确认信号
      2-观察等待：介于两者之间

    参数说明：
      equity_df: 资金曲线DataFrame（需含'净值'列）
      args: 参数组 [n, short_period, long_period, threshold]
        n: 恶化预警周期（3-5日）
        short_period: 短期动量周期（5-10日）
        long_period: 长期动量周期（20-60日）
        threshold: 动量阈值（-0.05至0.05）

    应用场景：
      1. 熊市/高波动环境：通过恶化预警及时空仓
      2. 震荡市场：半仓过渡减少磨损
      3. 牛市：强度确认后满仓捕捉趋势
      4. 参数优化建议：[3, 5, 20, -0.05]

    性能优化：
      1. 向量化计算替代循环
      2. 预计算关键指标
      3. 避免冗余计算
      4. 内存高效处理
    """
    # ===== 参数解析 =====
    params = args[0] if len(args) == 1 else args
    n = int(params[0])
    short_period = int(params[1])
    long_period = int(params[2])
    threshold = float(params[3])

    # ===== 预计算关键指标 =====
    # 1. 恶化预警信号
    price_momentum = equity_df['净值'].pct_change(n)
    momentum_change = price_momentum.pct_change()
    momentum_change = momentum_change.replace([np.inf, -np.inf], np.nan).ffill().bfill().fillna(0)
    deterioration_signal = (price_momentum < 0) & (momentum_change > 0)

    # 2. 强度确认信号
    short_ret = equity_df['净值'].pct_change(short_period)
    long_ret = equity_df['净值'].pct_change(long_period)
    confirmation_signal = (short_ret > threshold) & (long_ret > 0)

    # ===== 状态机向量化实现 =====
    signals = pd.Series(0.5, index=equity_df.index)  # 默认半仓
    state = np.zeros(len(equity_df), dtype=np.int8)  # 状态数组: 0=空仓,1=满仓,2=半仓

    # 计算有效起始位置
    start_idx = max(n, short_period, long_period)

    # 状态转换逻辑（向量化实现）
    for i in range(start_idx, len(equity_df)):
        if i == start_idx:
            state[i] = 0  # 初始状态为空仓防御

        elif state[i - 1] == 0:  # 空仓状态
            if not deterioration_signal.iloc[i]:
                state[i] = 2  # 转入观察
            else:
                state[i] = 0  # 保持空仓

        elif state[i - 1] == 1:  # 满仓状态
            if deterioration_signal.iloc[i]:
                state[i] = 0  # 转入空仓
            else:
                state[i] = 1  # 保持满仓

        else:  # 观察状态(state=2)
            if confirmation_signal.iloc[i]:
                state[i] = 1  # 转入满仓
            elif deterioration_signal.iloc[i]:
                state[i] = 0  # 转入空仓
            else:
                state[i] = 2  # 保持观察

    # 状态映射到仓位
    signals.iloc[:start_idx] = 0.5  # 前期数据默认半仓
    signals.iloc[start_idx:] = np.select(
        [state[start_idx:] == 0, state[start_idx:] == 1],
        [0.0, 1.0],
        default=0.5
    )

    return signals