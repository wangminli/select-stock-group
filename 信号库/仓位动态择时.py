"""1. 仓位计算原理 这个择时因子特别适合追求"下跌时少亏，上涨时多赚"的投资者
当近期收益为正时：保持满仓（仓位=1.0）
当近期收益为负时：使用指数函数计算仓位比例
仓位 = (e^(-gain * |跌幅|) - 1) / (e - 1)
2. 参数说明
n：观察周期（计算多少天的涨跌幅）
gain：仓位调整敏感度（控制仓位随跌幅变化的速率）
3. 仓位变化特性
小幅下跌：仓位快速下降（风险规避）
大幅下跌：仓位缓慢下降（价值投资思维）
零跌幅：仓位=0（理论值，实际因pct=0不触发计算）
正收益：保持满仓（顺势而为）

平衡型配置n = 10  # 中期观察窗口gain = 10 # 中等敏感度
进取型配置（高风险偏好）n = 20  # 长期观察窗口  gain = 5  # 低敏感度

"""

import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:


    # ===== 获取策略参数
    if isinstance(args[0], tuple):
        n,gain = args[0]
    else:
        n,gain = args

    # # ===== 计算近期涨跌幅
    pct = equity_df["净值"].pct_change(n)
 
    # 计算信号
    # 空仓(0)，默认满仓(1)
    signals = pd.Series(1, index=equity_df.index)
    #涨幅大于等于零时，持仓比例为1，满仓
    #涨幅小于零时，持仓比例为(pow(e,-x)-1)/(e-1)，指数控仓，跌100%时，满仓
    signals[pct < 0] =    (np.exp(-gain*pct[pct < 0])-1.0)/(np.exp(1.0)-1.0)

    # print("止损信号：", signals.describe())
    
    return signals
