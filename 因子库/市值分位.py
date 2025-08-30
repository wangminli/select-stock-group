"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""
import pandas as pd

# from config import data_path
# 如需新增另外的文件匹配数据，如财务因子列


def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    """
    计算并将新的因子列添加到股票行情数据中，并返回包含计算因子的DataFrame及其聚合方式。

    工作流程：
    1. 根据提供的参数计算股票的因子值。
    2. 将因子值添加到原始行情数据DataFrame中。
    3. 定义因子的聚合方式，用于周期转换时的数据聚合。

    :param df: pd.DataFrame，包含单只股票的K线数据，必须包括市场数据（如收盘价等）。
    :param param: 因子计算所需的参数，格式和含义根据因子类型的不同而有所不同。
    :param kwargs: 其他关键字参数，包括：
        - col_name: 新计算的因子列名。
        - fin_data: 财务数据字典，格式为 {'财务数据': fin_df, '原始财务数据': raw_fin_df}，其中fin_df为处理后的财务数据，raw_fin_df为原始数据，后者可用于某些因子的自定义计算。
        - 其他参数：根据具体需求传入的其他因子参数。
    :return: tuple
        - pd.DataFrame: 包含新计算的因子列，与输入的df具有相同的索引。
        - dict: 聚合方式字典，定义因子在周期转换时如何聚合（例如保留最新值、计算均值等）。

    注意事项：
    - 如果因子的计算涉及财务数据，可以通过`fin_data`参数提供相关数据。
    - 聚合方式可以根据实际需求进行调整，例如使用'last'保留最新值，或使用'mean'、'max'、'sum'等方法。
    """

    # ======================== 参数处理 ===========================
    # 从kwargs中提取因子列的名称，这里使用'col_name'来标识因子列名称
    col_name = kwargs.get('col_name', '市值分位')
    # 计算市值分位数（按交易日期分组后计算百分位）

    # data = pd.read_csv(data_path / 'finance_data.csv') 在此处直接添加列

    df[col_name] = df['总市值'].transform(lambda x: x.rank(pct=True))

    # 定义周期聚合方式（保留最后一天的值）
    agg_rules = {col_name: 'last'}

    return df[[col_name]], agg_rules