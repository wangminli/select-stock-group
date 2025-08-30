'''
1. 参数定义
N1 、 N2 、 N3 ：RSI 的计算周期（通常为短期、中期、长期周期）。
M ：RSI 的阈值，用于判断是否看涨（通常为 RSI 的超买/超卖阈值）。
2. 参数推荐值
(1) RSI 周期 ( N1 、 N2 、 N3 )
短期周期 ( N1 )：6 或 12
用于捕捉短期价格波动。
中期周期 ( N2 )：14（经典 RSI 默认值）
平衡短期和长期信号。
长期周期 ( N3 )：24 或 30
用于过滤长期趋势。
(2) 阈值 ( M )
推荐值：50 或 60
RSI >= 50 ：表示价格处于中性或强势区域。
RSI >= 60 ：更严格的看涨条件，避免假信号。
3. 参数组合示例
示例 1（保守策略）
N1=6 , N2=14 , N3=24 , M=50
短期 RSI 上穿中期 RSI，且长期 RSI 不低于 50 时看涨。
示例 2（激进策略）
N1=6 , N2=14 , N3=30 , M=60
短期 RSI 上穿中期 RSI，且长期 RSI 不低于 60 时看涨（减少假信号）。
4. 调参建议
回测验证：
通过历史数据回测，调整参数以优化收益和风险比。
市场适应性：
震荡市：缩短周期（如 N1=6 , N2=12 ）。
趋势市：拉长周期（如 N1=12 , N2=24 ）。
避免过拟合：
不要追求过高的回测收益，需确保参数在样本外数据中有效。
'''
import pandas as pd
from MyTT import *

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
    # print(df)  # 打印预处理数据
    # print(param)  # 打印因子参数
    # print(kwargs)  # 打印其他参数（字典，包括：参数,列名）{'col_name': 'RSI_24'}（见《8.5：34分》）
    # exit()
    # ======================== 参数处理 ===========================
    # 从kwargs中提取因子列的名称，这里使用'col_name'来标识因子列名称（不管）,即新增列名为因子名
    col_name = kwargs['col_name']
    N1 = param[0]
    N2 = param[1]
    N3 = param[2]
    M = param[3]

    # ======================== 计算因子 ===========================
    # 基础数据定义
    CLOSE = df['收盘价_复权']
    LC = REF(CLOSE, 1);
    RSI1 = SMA(MAX(CLOSE - LC, 0), N1, 1) / SMA(ABS(CLOSE - LC), N1, 1) * 100
    RSI2 = SMA(MAX(CLOSE - LC, 0), N2, 1) / SMA(ABS(CLOSE - LC), N2, 1) * 100
    RSI3 = SMA(MAX(CLOSE - LC, 0), N3, 1) / SMA(ABS(CLOSE - LC), N3, 1) * 100
    # 如果RSI1>RSI2或者RSI3>=50，则'RSI_QS'值为1，否则为0
    RSI_QS = ((RSI1 > RSI2) | (RSI3 >= M)).astype(int)

    df[col_name] = RSI_QS

    # ======================== 聚合方式 ===========================
    # 'last'表示在周期转换时，保留该因子列中的最新值(最后一个值)
    agg_rules = {col_name: 'last'}

    # 返回新计算的因子列以及因子聚合方式
    return df[[col_name]], agg_rules