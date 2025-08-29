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


def add_factor(df: pd.DataFrame, param, **kwargs) -> (pd.DataFrame, dict):
    """
        计算 (下影线 / 实体) * 成交量 的 N 期增长率因子（强调下影线相对强度）。

        :param df: 包含行情数据的 DataFrame，必须包括：
                   - '开盘价'
                   - '收盘价'
                   - '最低价'
                   - '成交量'
        :param param: 增长率周期 N
        :param kwargs: 必须包含 col_name: 新因子列名
        :return: tuple (DataFrame[新因子列], 聚合规则 dict)
        """
    col_name = kwargs['col_name']
    # N = int(param)  # 回看区间
    # N = int(param) if param is not None else 1  # 默认周期为1

    # 1. 实体大小 = |收盘价 - 开盘价|
    body = (df['收盘价_复权'] - df['开盘价_复权']).abs()

    # 防止除零：实体为0时，比例设为0
    body = body.replace(0, 1e-8)

    # 2. 下影线 = min(开盘, 收盘) - 最低价
    lower_shadow = df[['开盘价_复权', '收盘价_复权']].min(axis=1) - df['最低价_复权']

    # 3. 下影线占比 = 下影线 / 实体
    ratio = lower_shadow / body

    # 4. 占比 × 成交量
    weighted = ratio * df['成交量']

    # 5. N 期增长率（可视为情绪增强动量）
    # growth = weighted.pct_change(N)

    df[col_name] = weighted
    agg_rules = {col_name: 'last'}

    return df[[col_name]], agg_rules
