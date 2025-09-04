"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""
import pandas as pd

# 财务因子列：此列表用于存储财务因子相关的列名称
fin_cols = []  # 财务因子列，配置后系统会自动加载对应的财务数据

def add_factor(df: pd.DataFrame, param, fin_data=None, **kwargs) -> (pd.DataFrame, dict):
    """
    计算WR指标并检测底背离

    工作流程：
    1. 计算WR(威廉指标)
    2. 检测价格和WR的底背离
    3. 将背离强度添加到原始数据中

    :param df: pd.DataFrame，包含单只股票的K线数据，必须包括市场数据（如收盘价、最高价、最低价等）。
    :param param: 因子计算所需的参数，WR指标的计算周期。
    :param fin_data: 财务数据字典，格式为 {'财务数据': fin_df, '原始财务数据': raw_fin_df}（本因子不使用）。
    :param kwargs: 其他关键字参数，包括：
        - col_name: 新计算的因子列名。
    :return: tuple
        - pd.DataFrame: 包含新计算的因子列，与输入的df具有相同的索引。
        - dict: 聚合方式字典，定义因子在周期转换时如何聚合。

    注意事项：
    - WR底背离：当价格创新低但WR没有创新高（即没有更超卖）时，出现底背离信号。
    - 聚合方式使用'last'保留最新值。
    """

    # ======================== 参数处理 ===========================
    # 从kwargs中提取因子列的名称
    col_name = kwargs['col_name']

    # ======================== 计算WR指标 ===========================
    # 前置计算
    highest_high = df['最高价'].rolling(param).max()
    lowest_low = df['最低价'].rolling(param).min()
    wr = (highest_high - df['收盘价']) / (highest_high - lowest_low) * 100
    wr.fillna(0, inplace=True)
    df[col_name] = wr > 1
    # df[col_name] = (wr > 1) & ((wr - 20).abs() > 1) & ((wr - 50).abs() > 1)

    # ======================== 聚合方式 ===========================
    # 定义因子聚合方式，这里使用'last'表示在周期转换时保留该因子的最新值
    agg_rules = {
        col_name: 'last'  # 'last'表示在周期转换时，保留该因子列中的最新值
        # 如果需要其他聚合方式，可以选择以下函数：
        # - 'mean'：计算均值，例如用于背离强度的平均值。
        # - 'max'：计算最大值，例如用于最大背离强度。
        # - 'min'：计算最小值，例如用于最小背离强度。
        # - 'sum'：计算总和，例如用于背离次数。
    }

    # 返回新计算的因子列以及因子聚合方式
    return df[[col_name]], agg_rules 