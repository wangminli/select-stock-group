""""【Runist】基于市场宽度的择时策略  市值最小的前20%的股票  上涨股票占比\上涨中位数"""

import pandas as pd
import numpy as np

from core.utils.path_kit import get_file_path
from typing import Dict

from core.model.backtest_config import load_config


def get_small_cap_df(all_stock_df: pd.DataFrame, percentile: float = 0.2) -> pd.DataFrame:
    # 确保日期格式正确
    all_stock_df['交易日期'] = pd.to_datetime(all_stock_df['交易日期'])

    # 按交易日分组，并保留每组中市值最小的前 20%
    def select_small_cap(group):
        group = group.sort_values("总市值")
        n = int(len(group) * percentile)
        return group.iloc[:max(n, 1)]  # 至少保留一只股票，避免空组

    small_cap_df = all_stock_df.groupby("交易日期", group_keys=False).apply(select_small_cap)

    return small_cap_df


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    backtest_config = load_config()
    index_data = backtest_config.read_index_with_trading_date()

    # 读取所有股票的预处理数据
    candle_df_dict: Dict[str, pd.DataFrame] = pd.read_pickle(
        get_file_path('data', '运行缓存', '股票预处理数据.pkl')
    )

    # ===== 计算市场广度指标（上涨家数、中位涨跌幅、上涨占比）
    all_stock_df = pd.concat(
        [df.assign(涨跌幅=df['收盘价_复权'].pct_change(1))
         for df in candle_df_dict.values()],
        axis=0,
        ignore_index=True
    )

    # 删除月末为退市风险的周期数
    cond = ~all_stock_df['股票名称'].str.contains('ST|S|\\*|退|bj', regex=False)  #'ST|S|\\*|退|sz3|sh688|bj'

    all_stock_df = all_stock_df[cond]
    small_cap_df = get_small_cap_df(all_stock_df, percentile=0.2)

    small_cap_df["是否上涨"] = small_cap_df["涨跌幅"] > 0
    small_cap_df["是否下跌"] = small_cap_df["涨跌幅"] < 0

    breadth_df = small_cap_df.groupby("交易日期").agg({
        "是否上涨": "sum",
        "是否下跌": "sum",
        "股票代码": "count",
        "涨跌幅": lambda x: x.median()
    }).rename(columns={
        "是否上涨": "上涨家数",
        "是否下跌": "下跌家数",
        "股票代码": "总股票数",
        "涨跌幅": "中位涨跌幅"
    })

    breadth_df["上涨占比"] = breadth_df["上涨家数"] / breadth_df["总股票数"]
    # breadth_df["广度差"] = breadth_df["上涨家数"] - breadth_df["下跌家数"]

     # vt = beta * vt-1 + (1-beta) * x
    beta = 0.9
    span = 2 / (1 - beta) - 1
    breadth_df["上涨占比_ema"] = breadth_df["上涨占比"].ewm(span=span).mean()
    breadth_df["中位涨跌幅_ema"] = breadth_df["中位涨跌幅"].ewm(span=span).mean()

    n = 5
    breadth_df["ema_max"] = breadth_df["上涨占比_ema"].rolling(n).max()
    breadth_df["ema_min"] = breadth_df["上涨占比_ema"].rolling(n).min()
    breadth_df["ema_mean"] = breadth_df["上涨占比_ema"].rolling(n).mean()

    equity_df["交易日期"] = pd.to_datetime(equity_df["交易日期"])
    breadth_df.index = pd.to_datetime(breadth_df.index)
    breadth_df_reset = breadth_df.reset_index()
    temp_df = pd.merge(equity_df, breadth_df_reset, on="交易日期", how="left")
    # temp_df.to_csv("ADR.csv", encoding="utf_8_sig")

    # ===== 合并指数数据
    index_data.set_index("交易日期", inplace=True)
    combined = index_data.join(breadth_df[["上涨占比_ema", "中位涨跌幅_ema"]], how="inner")
    # combined = index_data.join(breadth_df[["上涨占比_ema", "中位涨跌幅_ema", "ema_max", "ema_min"]], how="inner")
    # combined = index_data.join(breadth_df[["上涨占比_ema", "ema_mean"]], how="inner")
    # combined = index_data.join(breadth_df[["上涨占比", "上涨占比_ema"]], how="inner")


    # ===== 构建择时信号
    def judge_signal(row):

        if row["上涨占比_ema"] > 0.45 and row["中位涨跌幅_ema"] > 0.01:
            return 1.0  # 情绪极好，满仓
        elif row["上涨占比_ema"] > 0.35:
            return 0.8  # 情绪偏强
        elif row["上涨占比_ema"] > 0.25:
            return 0.5  # 中性
        elif row["上涨占比_ema"] < 0.15 and row["中位涨跌幅_ema"] < 0:
            return 0.2  # 情绪差
        else:
            return 0.0  # 情绪极差

        
    combined["择时信号"] = combined.apply(judge_signal, axis=1).ffill().fillna(1.0).shift(1)        # shift(1)  是因为拿到了当天的数据，应该提前一天判断

    # ===== 映射到股票上
    signals = pd.Series(np.nan, index=equity_df.index)
    mapped_signals = equity_df["交易日期"].map(combined["择时信号"])
    signals.loc[~mapped_signals.isna()] = mapped_signals.dropna()

    return signals