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


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    基于止损的择时信号（只有满仓或空仓两种状态）
    当资金曲线在近期跌幅超过阈值时，空仓
    其他情况，满仓
    
    :param equity_df: 资金曲线的DF
    :param args: 其他参数
        args[0]: 止损观察周期
        args[1]: 止损阈值
    :return: 返回包含 leverage 的数据
    """

    # ===== 获取策略参数
    df = pd.read_csv(r'E:\game\data\micro_cap_index_pb\micro_cap_index_pb.csv', encoding='gbk')
    df['交易日期'] = pd.to_datetime(df['交易日期'])
    equity_df['交易日期'] = pd.to_datetime(equity_df['交易日期'])
    merged_df = pd.merge(equity_df, df, on='交易日期', how='left')
    signals = pd.Series(None, index=merged_df.index)


    buy = merged_df[merged_df["3年分位点"] < 30]  # 微盘股指数pb<2.15买入  有带着未来数据去调参的成分
    signals.loc[buy.index] = 1
    sell = merged_df[merged_df["3年分位点"] > 90]  # 微盘股指数pb>2.33卖出  有带着未来数据去调参的成分
    signals.loc[sell.index] = 0
    signals = signals.ffill()  # 信号向前补全
    signals.fillna(1, inplace=True)  # 开头周期空缺填补

    # max_draw = merged_df[
    #     merged_df["净值dd2here"] < -0.055]  # 微盘股指数pb>2.33触发卖出后，回撤大于5.5个点就提前抄底，回到6个点内就卖出，直到微盘股指数pb<2.15买入
    # signals.loc[max_draw.index] = 1
    #
    # in1or4 = merged_df[merged_df["交易日期"].dt.month.isin([1])]  # 排出1月份优先级最高，覆盖其他择时条件
    # signals.loc[in1or4.index] = 0.0
    return signals
