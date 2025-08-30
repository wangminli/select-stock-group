
import pandas as pd

fin_cols = []  # 财务因子列


# noinspection PyUnusedLocal
def add_factor(df: pd.DataFrame, param, fin_data=None, **kwargs) -> (pd.DataFrame, dict):
    """
    计算KDJ指标并生成买入信号

    :param df: DataFrame，包含股票数据的DataFrame
    :param col_name: str，用于存储买入信号的列名
    :param N: int，计算最高价、最低价和收盘价的窗口期，默认为9
    :param M1: int，计算K值的窗口期，默认为3
    :param M2: int，计算D值的窗口期，默认为3
    :return: DataFrame，包含买入信号的DataFrame
    """
    # ======================== 参数处理 ===========================
    # 从kwargs中提取因子列的名称，这里使用'col_name'来标识因子列名称（不管）,即新增列名为因子名
    col_name = kwargs['col_name']
    # 从param中提取因子计算参数
    N = param[0]
    M = param[1]

    # ======================== 计算因子 ===========================
    # 基础数据定义
    # CCI计算公式
    # CCI = (收盘价 - N周期内收盘价的均值) / (N周期内收盘价的标准差 * 0.015)
    # 计算典型价格
    df['typical_price'] = (df['最高价_复权'] + df['最低价_复权'] + df['收盘价_复权']) / 3

    # 计算典型价格的移动平均
    df['moving_avg'] = df['typical_price'].rolling(window=N).mean()

    # 计算平均偏差
    rolling_mean = df['typical_price'].rolling(window=N).mean()
    df['mean_deviation'] = (
        (df['typical_price'] - rolling_mean)
        .abs()
        .rolling(window=N)
        .mean()
    )

    # 计算CCI
    df['cci'] = (df['typical_price'] - df['moving_avg']) / (0.015 * df['mean_deviation'])

    df[col_name] = 0  # 默认没有买入信号
    df.loc[(df['cci'] < M), col_name] = 1  # D小于20且K上穿D时产生买入信号


    # ======================== 聚合方式 ===========================
    # 'last'表示在周期转换时，保留该因子列中的最新值(最后一个值)
    agg_rules = {col_name: 'last'}

    # 返回新计算的因子列以及因子聚合方式
    return df[[col_name]], agg_rules