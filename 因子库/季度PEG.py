"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""
import pandas as pd
from typing import Tuple, Dict, Optional
import numpy as np

fin_cols = ['R_revenue@xbx_单季同比','R_np@xbx_ttm']  # 财务因子列


# noinspection PyUnusedLocal
def add_factor(df: pd.DataFrame, param, fin_data=None, **kwargs) -> (pd.DataFrame, dict):
    # 从额外参数中获取因子名称
    col_name = kwargs['col_name']
    
    # ========== 计算PE ==========
    # 处理分母为0或负值的情况
    valid_mask = (df['R_np@xbx_ttm'] > 0)  # 仅当净利润为正时计算
    df['PE'] = np.where(
        valid_mask,
        df['总市值'] / (df['R_np@xbx_ttm'] ), 
        0  # 无效值设为0
    )
    df['PE'] = df['PE'].astype(float)

    # 计算滚动PE
    # 处理极端值（PE超过1000视为无效）
    df['PE'] = df['PE'].where(df['PE'] <= 2000, 0)

    # 计算PEG
    df['营收同比'] = df['R_revenue@xbx_单季同比']

    df['PEG'] = df['PE'] / df['营收同比']/100

    # 计算滚动PEG
    df[col_name] = df['PEG']

    # 处理极端值（PEG超过1000视为无效）
    #df[col_name] = df[col_name].where(df[col_name] <= 1000, 0)
    # ========== 返回结果 ==========
    factor_df = df[[col_name]]
    agg_dict = {col_name: 'last'}

    return factor_df, agg_dict