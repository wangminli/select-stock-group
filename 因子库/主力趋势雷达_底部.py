
import pandas as pd
import numpy as np

# 财务因子列：此列表用于存储财务因子相关的列名称
fin_cols = []  # 财务因子列，配置后系统会自动加载对应的财务数据


def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    """
    1、前‘m’天内没有红柱；
    2、‘CC’不再创新低；
    3、‘CC’小于‘n’；
    """

    # ======================== 参数处理 ===========================
    # 从kwargs中提取因子列的名称，这里使用'col_name'来标识因子列名称
    col_name = kwargs['col_name']
    # ---------- 参数 ---------
    m = param[0]  # 多少日内没有红柱的情况
    n = param[1] / 10  # CC小于n
    # a = param[2]  # CC和K线连跌数量

    # ======================== 计算因子 ===========================
    # 我们选取在超卖线以下，且‘CC’小于‘平均数’
    # ==============不用talib计算=======================
    df['最小值'] = df['最低价'].rolling(window=10).min()
    df['最大值'] = df['最高价'].rolling(window=25).max()
    df['CC'] = round(
        ((df['收盘价'] - df['最小值']) / (df['最大值'] - df['最小值']) * 4).ewm(span=4, adjust=False).mean(), 2)
    df['平均线'] = round(df['CC'].ewm(span=3, adjust=False).mean(), 2)
    # ============== 因子条件 ===================
    df['是否红柱'] = np.where(df['CC'] >= df['平均线'], 1, 0)
    # df['是否CC下跌'] = np.where(df['CC'].diff() < 0, 1, 0)
    # df['是否K线下跌'] = np.where(df['收盘价'].diff() < 0, 1, 0)

    # ------------- 因子条件1 -------------
    condition1 = (df['是否红柱'].rolling(window=m).max() == 0) & (df['CC'] >= df['CC'].shift(1))  # 过去m日无红柱且CC止跌
    condition3 = (df['CC'] <= n)  # CC小于等于阈值n
    condition5 = (df['是否红柱'].rolling(window=m).max() == 1)  # 过去m日内有红柱
    # condition5 = (df['是否CC大跌'].rolling(window=m).max() == 0)  # 过去m日内无CC大跌
    # ------------- 因子条件2 -------------
    condition4 = (df['CC'] > n)  # CC大于阈值n
    # condition6 = (df['是否CC下跌'].rolling(window=a).sum() == a) & (df['是否K线下跌'].rolling(window=a).sum() == a)  # 过去a日内CC和K线连跌

    # 明确组合条件优先级
    df[col_name] = np.where(((condition1 | condition5) & condition3) | condition4, 1, 0)
    # 因子条件2补充，CC和K线连跌4个的排除
    # df[col_name] = np.where(condition4 & condition6, 0, df[col_name])

    # ======================== 聚合方式 ===========================
    # 定义因子聚合方式，这里使用'last'表示在周期转换时保留该因子的最新值
    agg_rules = {
        col_name: 'last'  # 'last'表示在周期转换时，保留该因子列中的最新值
        # 如果需要其他聚合方式，可以选择以下函数：
        # - 'mean'：计算均值，例如用于市值的平均值。
        # - 'max'：计算最大值，例如用于最大涨幅。
        # - 'min'：计算最小值，例如用于最大跌幅。
        # - 'sum'：计算总和，例如用于成交量。
    }

    # 返回新计算的因子列以及因子聚合方式
    return df[[col_name]], agg_rules
