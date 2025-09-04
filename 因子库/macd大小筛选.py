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


def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):

    # ======================== 参数处理 ===========================
    col_name = kwargs['col_name']  # 获取因子列名-
    fast_period = int(param[0])  # 快速EMA的周期
    slow_period = int(param[1])  # 慢速EMA的周期
    signal_period = int(param[2])
    # 计算MACD指标
    ema_fast = df['收盘价_复权'].ewm(span=fast_period, adjust=False).mean()
    ema_slow = df['收盘价_复权'].ewm(span=slow_period, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
    # 计算MACD柱状图
    macd_hist = macd_line - signal_line

    # macd_hist不能是最近60期的最大值或者最小值：
    crossover = (macd_hist == macd_hist.rolling(window=int(param[3])).max()) | (macd_hist == macd_hist.rolling(window=int(param[3])).min())

    df[col_name] = crossover.astype(int)

    agg_rules = {col_name: 'last'}  # 周期转换时保留最新值

    return df[[col_name]], agg_rules