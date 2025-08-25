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

# 财务因子列：此列表用于存储财务因子相关的列名称
fin_cols = []  # 财务因子列，配置后系统会自动加载对应的财务数据
import pandas as pd

def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    """
    计算 LOWER_SHADOW * VOLUME 序列的 N 期收益率因子。

    :param df: 包含行情数据的 DataFrame，必须至少包括以下列：
               - '开盘价'
               - '收盘价'
               - '最低价'
               - '成交量'
    :param param: 计算收益率的周期 N（如 NUMBER_24 则传入 24）
    :param kwargs: 其他关键字参数，必须包含：
                   - col_name: 新计算因子列名
    :return: tuple
             - pd.DataFrame: 仅包含新计算的因子列
             - dict: 因子在聚合时的聚合方式，通常取 'last'
    """

    # 从 kwargs 中提取因子列名
    col_name = kwargs['col_name']
    # 将 param 转为整数周期
    N = int(param) if param is not None else 1

    # 1. 计算下影线 = min(开盘价, 收盘价) − 最低价
    lower_shadow = df[['开盘价', '收盘价']].min(axis=1) - df['最低价']

    # 2. 计算下影线与成交量的乘积
    prod = lower_shadow * df['成交量']

    # 3. 对乘积序列计算 N 期收益率
    returns = prod.pct_change(N)

    # 4. 将结果写入 DataFrame，并定义聚合方式
    df[col_name] = returns
    agg_rules = {col_name: 'last'}

    return df[[col_name]], agg_rules
