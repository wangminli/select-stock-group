"""
这个是从 3日涨跌幅择时.py改过来的——2025-08-25
"""
"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""

import pandas as pd


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    根据资金曲线，动态调整杠杆
    :param equity_df: 资金曲线的DF
    :param args: 其他参数
    :return: 返回包含 leverage 的数据
    """
    # ===== 计算指标

    # 默认满仓
    signals = pd.Series(1.0, index=equity_df.index)
    condition = equity_df['净值'].pct_change( int(args[0])) < -0.07
    signals[condition] = 0.0
    signals.fillna(1.0, inplace=True)

    return signals
