import pandas as pd
import numpy as np


def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
   # ======================== 参数处理 ===========================
    # 从kwargs中提取因子列的名称，这里使用'col_name'来标识因子列名称
    col_name = kwargs['col_name']

    # ======================== 计算因子 ===========================

    # 计算因子值并处理无效情况
    with pd.option_context('mode.use_inf_as_na', True):
        df[col_name] = df['开盘价'] / df['最低价'] - 1
        # 处理除数为0或无效值的情况
        df[col_name] = df[col_name].replace([np.inf, -np.inf], np.nan)
        # 填充NaN值为0或根据策略需求处理
        df[col_name] = df[col_name].fillna(0)


    # ======================== 聚合方式 ===========================
    # 定义因子聚合方式，这里使用'last'表示在周期转换时保留该因子的最新值
    agg_rules = {
        col_name: 'last'  # 'last'表示在周期转换时，保留该因子列中的最新值
        # 如果需要其他聚合方式，可以选择以下函数：
        # - 'mean'：计算均值，例如用于市值的平均值。
        # - 'max'：计算最大值，例如用于最大涨幅。
        # - 'min'：计算最小值，例如用于最大跌幅。
        # - 'sum'：计算总和，例如用于成交量。
    }
    # 返回因子列和聚合规则（此处为空）
    return df[[col_name]], agg_rules
