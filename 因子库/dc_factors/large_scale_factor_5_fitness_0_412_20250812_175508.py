"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行

large_scale_factor_5_fitness_0_412_20250812_175508: 基于遗传规划算法生成的因子5，适应度: 0.411648
适应度分数: 0.411648
原始GP表达式: TS_MA(MAXIMUM(HIGH_CLOSE_DEVIATION, DAILY_RANGE_RATIO), NUMBER_24)
生成时间: 2025-08-13 00:28:19
"""

import pandas as pd
import numpy as np

# 财务因子列：此列表用于存储财务因子相关的列名称
fin_cols = []  # 财务因子列，配置后系统会自动加载对应的财务数据

def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    """
    计算因子，使用八个输入列：开盘价、最高价、最低价、收盘价、成交量、成交额、流通市值、总市值
    """
    col_name = kwargs.get('col_name', 'large_scale_factor_5_fitness_0_412_20250812_175508')
    
    # 提取基础数据列
    o = df['开盘价']
    h = df['最高价']
    l = df['最低价']
    c = df['收盘价']
    v = df['成交量']
    amt = df['成交额']
    mc_circ = df['流通市值']
    mc_total = df['总市值']
    
    # 计算因子值
    factor_value = ((np.maximum((h - c) / c.replace(0, np.nan), (h - l) / c.replace(0, np.nan))).rolling(window=24, min_periods=1).mean())  # 原始GP表达式: TS_MA(MAXIMUM(HIGH_CLOSE_DEVIATION, DAILY_RANGE_RATIO), NUMBER_24)

    # 确保结果是Series类型
    if not isinstance(factor_value, pd.Series):
        if np.isscalar(factor_value):
            factor_value = pd.Series([factor_value] * len(df), index=df.index)
        else:
            factor_value = pd.Series(factor_value, index=df.index)

    # 处理无效值（若可用）
    if hasattr(factor_value, 'replace'):
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)

    # 将结果添加到DataFrame
    df[col_name] = factor_value
    
    # 定义聚合方式
    agg_rules = {col_name: 'last'}  # 使用最后一个值
    
    # 返回新计算的因子列以及聚合方式
    return df[[col_name]], agg_rules
