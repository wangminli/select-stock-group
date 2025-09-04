import pandas as pd
import numpy as np

fin_cols = []  # 财务因子列


# noinspection PyUnusedLocal
def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    """
    计算并将新的因子列添加到股票行情数据中，并返回包含计算因子的DataFrame及其聚合方式。

    工作流程：
    1. 根据提供的参数计算股票的因子值。
    2. 将因子值添加到原始行情数据DataFrame中。

    :param df: pd.DataFrame，包含单只股票的K线数据，必须包括市场数据（如收盘价等）。
    :param param: 因子计算所需的参数，格式和含义根据因子类型的不同而有所不同。
    :param kwargs: 其他关键字参数，包括：
        - col_name: 新计算的因子列名。
        - fin_data: 财务数据字典，格式为 {'财务数据': fin_df, '原始财务数据': raw_fin_df}，其中fin_df为处理后的财务数据，raw_fin_df为原始数据，后者可用于某些因子的自定义计算。
        - 其他参数：根据具体需求传入的其他因子参数。
    :return:
        - pd.DataFrame: 包含新计算的因子列，与输入的df具有相同的索引。

    注意事项：
    - 如果因子的计算涉及财务数据，可以通过`fin_data`参数提供相关数据。
    """
    # 从额外参数中获取因子名称
    # 计算因子时需要用到的短参数
    n = float(param)

    col_name = kwargs['col_name']
    df['满足跌幅'] = np.where((df['收盘价_复权'].pct_change(1) * 100) < n,True,False)
    df['收在最低'] = np.where(df['收盘价_复权'] == df['最低价_复权'],True,False)
    df[col_name] = np.where((df['收在最低'] & df['满足跌幅']),1,0)
    # 定义因子聚合方式，这里选择获取最新的因子值
    agg_dict = {col_name: 'last'}

    return df[[col_name]], agg_dict
