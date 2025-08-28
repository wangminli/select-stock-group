

import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
策略逻辑说明：
波动率估算：
使用净值每日绝对变化(|净值_t - 净值_{t-1}|)作为波动率的基础
计算该值的移动平均作为波动率指标
这反映了净值序列自身的波动特性
通道构建：
中轨 = 净值在波动率周期内的移动平均
上轨 = 中轨 + 波动率 × 乘数
下轨 = 中轨 - 波动率 × 乘数
通道宽度自适应净值波动水平
交易信号：
当净值突破上轨时 → 开多仓(1)
当净值跌破下轨时 → 平仓(0)
信号具有延续性，未触发信号时保持前一日状态
初始状态默认空仓(0)
    """
    # 参数解析
    vol_period = int(args[0])
    multiplier = float(args[1])

    # 验证数据列
    if '净值' not in equity_df.columns:
        raise ValueError("缺少必要数据列: '净值'")

    # 计算净值波动率 (替代ATR)
    df = equity_df.copy()
    df['daily_change'] = df['净值'].diff().abs()  # 每日净值绝对变化
    df['volatility'] = df['daily_change'].rolling(vol_period).mean()  # 波动率估算

    # 计算净值通道
    ma_equity = df['净值'].rolling(vol_period).mean()  # 净值移动平均
    upper = ma_equity + df['volatility'] * multiplier  # 上轨
    lower = ma_equity - df['volatility'] * multiplier  # 下轨

    # 生成信号 (带状态延续)
    signals = pd.Series(np.nan, index=df.index)

    # 突破上轨做多
    signals[df['净值'] > upper] = 1

    # 跌破下轨平仓
    signals[df['净值'] < lower] = 0

    # 信号前向填充(保持持仓状态)
    signals = signals.ffill().fillna(0)  # 默认空仓

    return signals
