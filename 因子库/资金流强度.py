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

def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    """
    计算资金流强度因子：
    1. 计算每日资金流方向
    2. 计算资金流强度指标
    3. 识别资金流持续增强的股票

    参数:
    - param: [短期平滑窗口, 长期平滑窗口, 强度阈值]
    """
    # 参数处理
    col_name = kwargs['col_name']
    short_window = int(param[0])  # 短期平滑窗口
    long_window = int(param[1])  # 长期平滑窗口
    threshold = float(param[2])  # 强度阈值

    # 计算每日资金流方向
    typical_price = (df['最高价_复权'] + df['最低价_复权'] + df['收盘价_复权']) / 3
    money_flow = typical_price * df['成交量']

    # 计算每日资金流方向（正负号）
    money_flow_direction = np.sign(df['收盘价_复权'] - df['收盘价_复权'].shift(1))

    # 计算定向资金流
    directed_money_flow = money_flow * money_flow_direction

    # 计算短期和长期资金流强度
    short_mf = directed_money_flow.rolling(short_window).sum()
    long_mf = directed_money_flow.rolling(long_window).sum()

    # 计算资金流强度比率
    mf_ratio = short_mf / long_mf

    # 生成信号：资金流强度比率 > 阈值
    df[col_name] = (mf_ratio > threshold).astype(int)

    # 聚合规则
    agg_rules = {col_name: 'last'}

    return df[[col_name]], agg_rules