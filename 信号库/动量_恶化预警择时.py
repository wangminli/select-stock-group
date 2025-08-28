import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    动量恶化预警模型（关注下跌加速度）防御型策略
    核心逻辑是捕捉动量恶化信号（加速下跌）触发空仓，旨在严格规避回撤。
    使用单一动量周期 + 动量变化率（二阶导数）：
    空仓条件 = 动量负值（下跌） + 动量变化率为正（加速下跌）
    开仓条件 = 动量转正（简单阈值）
    状态机机制：空仓后需等待动量转正才能重新开仓，体现不对称风控。

    """
    # 获取参数
    n = int(args[0])  # 动量周期
  
    # 计算净值动量
    price_momentum = equity_df['净值'].pct_change(n)
  
    # 计算动量变化率
    momentum_change = price_momentum.pct_change()
  
    # 更合理的异常值处理：
    # 1. 将无穷大值设为NaN
    momentum_change = momentum_change.replace([np.inf, -np.inf], np.nan)
  
    # 2. 使用前向填充，保持趋势的连续性
    momentum_change = momentum_change.fillna(method='ffill')
  
    # 3. 如果开头有NaN，使用后向填充
    momentum_change = momentum_change.fillna(method='bfill')
  
    # 4. 如果仍有NaN（极少数情况），设为0
    momentum_change = momentum_change.fillna(0)
  
    # 初始化信号（默认做多）
    signals = pd.Series(1.0, index=equity_df.index)


    # 空仓条件：
    # 1. 净值动量为负（价格在下跌）
    # 2. 动量变化率为正（动量在增强，即加速下跌）
    # 3. 动量变化率高于阈值（加速下跌明显）
    exit_condition = (price_momentum < 0) & (momentum_change > 0)
  
    # 重新开仓条件：
    # 1. 净值动量为正（价格在上涨）
    reenter_condition = (price_momentum > 0)
  
    # 状态机：管理做多/空仓状态
    current_state = 1.0  # 1=做多，0=空仓
  
    for i in range(len(equity_df)):
        if current_state == 1.0:  # 当前是做多状态
            if exit_condition.iloc[i]:  # 满足空仓条件
                current_state = 0.0
        else:  # 当前是空仓状态
            if reenter_condition.iloc[i]:  # 满足重新开仓条件
                current_state = 1.0
      
        signals.iloc[i] = current_state
  
    # print("空仓次数：", signals[signals==0].count())
    # print("开仓次数：", signals[signals==1].count())
    # print(f"净值动量：最小值：{price_momentum.min():.6f} 最大值：{price_momentum.max():.6f} 中位数：{price_momentum.median():.6f}")
    # print(f"净值动量变化率：最小值：{momentum_change.min():.6f} 最大值：{momentum_change.max():.6f} 中位数：{momentum_change.median():.6f}")
  
    return signals 