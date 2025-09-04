"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""

import pandas as pd
import numpy as np
from statsmodels.sandbox.stats.runs import runstest_1samp


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    根据资金曲线，动态调整杠杆
    :param equity_df: 资金曲线的DF
    :param args: 其他参数
    :return: 返回包含 leverage 的数据
    """

    # ===== 获取策略参数
    window_size = 20

    # ===== 计算指标

    # 默认空仓
    signals = pd.Series(1.0, index=equity_df.index)

    # 自研策略2：继续空仓策略
    continue0 = (equity_df['净值'].pct_change() == 0) & (equity_df['净值'] - equity_df['净值'].rolling(60).max() < 0)
    signals.loc[continue0] = 0.0

    # 自研策略3：继续空仓策略2号
    signal_condition = signals.rolling(6).min() == 0
    equity_condition = equity_df['净值'].pct_change(3) < -0.1
    signals.loc[signal_condition & equity_condition] = 0.0

    # 自研策略：涨跌幅指标
    equity = equity_df['净值']
    equity_max6 = equity.rolling(6).max()
    equity_drawdown6 = (equity - equity_max6) / equity_max6
    equity_std = equity.rolling(window_size).apply(lambda x: (x / x.iloc[0]).std())
    equity_std_condition1 = (equity_std.rolling(window_size * 2).rank() > int(window_size * 2 - 4)) & (equity_drawdown6 < -0.04)
    signals.loc[equity_std_condition1] = 0.0

    # 借鉴策略1：动量策略
    ret2 = equity_df['净值'].pct_change(2)
    ret3 = equity_df['净值'].pct_change(3)
    ret4 = equity_df['净值'].pct_change(4)
    ret12 = equity_df['净值'].pct_change(12)
    equity_max10 = equity_df['净值'].rolling(10).max()
    strong_momentum = ((ret2 < -0.1) | (ret3 < -0.08) | (ret4 < -0.06) | (ret12 < -0.02)) & ((equity_df['净值'] - equity_max10) / equity_max10 > -0.1)
    signals.loc[strong_momentum] = 0.0

    signals.fillna(1.0, inplace=True)

    print("--------------------------------")
    print("signals.sum(): ", signals.sum())
    print("signals length: ", len(signals))
    print("--------------------------------")
    # equity_df.to_excel('./资金曲线-20250619.xlsx', sheet_name='资金曲线')

    return signals
