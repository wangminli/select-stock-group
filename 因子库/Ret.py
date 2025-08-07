"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""
import pandas as pd

fin_cols = []  # 财务因子列


# noinspection PyUnusedLocal
def add_factor(df: pd.DataFrame, param, fin_data=None, **kwargs) -> (pd.DataFrame, dict):
    """
    合并数据后计算策略所需的因子。

    :param df: 输入的K线数据，包含各类市场指标。
    :param param: 策略参数，用于配置因子计算的具体细节。
    :param fin_data: 财务数据字典，格式为 {'财务数据': fin_df, '原始财务数据': raw_fin_df}，
                     其中raw_fin_df包含需要舍弃的原始报告数据。
    :param kwargs: 其他关键字参数，包括但不限于因子名称（'col_name'）。
    :return:
        tuple:
            pd.DataFrame: 包含计算后的因子数据，索引与输入的df一致。
            dict: 聚合字典，指定因子数据的聚合方式。
    """
    # 从额外参数中获取因子名称
    col_name = kwargs['col_name']

    # 计算近期涨跌幅
    factor_col = df['收盘价_复权'].pct_change(param)

    # 创建包含指定因子的DataFrame
    factor_df = pd.DataFrame({col_name: factor_col}, index=df.index)
    # 定义因子聚合方式，这里选择获取最新的因子值
    agg_dict = {col_name: 'last'}

    return factor_df, agg_dict