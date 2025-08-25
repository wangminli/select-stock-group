"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""
import pandas as pd

# 财务因子列配置
fin_cols = [
    'B_total_assets@xbx',             # 总资产
    'B_total_equity_atoopc@xbx'       # 归属于母公司所有者权益合计
]

def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    """
    计算权益乘数(Equity Multiplier)因子
    
    工作流程：
    1. 计算股票的权益乘数值
    2. 将因子值添加到原始行情数据DataFrame中
    3. 定义因子的聚合方式，用于周期转换时的数据聚合
    
    :param df: 包含单只股票的K线数据和财务数据
    :param param: 因子计算参数（本因子不使用参数，但保留接口）
    :param kwargs: 其他参数，包含因子列名(col_name)等
    :return: 包含新因子的DataFrame和聚合规则字典
    
    计算公式：
    权益乘数 = 总资产 / 归属于母公司所有者权益合计

    就是ROE/ROA
    """
    # ======================== 参数处理 ===========================
    col_name = kwargs['col_name']  # 获取因子列名称
    
    # 权益乘数不使用参数，但保留param接口以保持框架一致性
    # 此处可以添加参数处理逻辑，例如不同计算方式的选择
    
    # ======================== 计算权益乘数因子 ====================
    # 权益乘数 = 总资产 / 归属于母公司所有者权益合计
    # - B_total_assets@xbx: 总资产
    # - B_total_equity_atoopc@xbx: 归属于母公司所有者权益合计
    
    # 避免除零错误
    denominator = df['B_total_equity_atoopc@xbx'].replace(0, pd.NA)
    df[col_name] = df['B_total_assets@xbx'] / denominator
    
    # ======================== 聚合方式 ===========================
    # 定义因子聚合方式，使用'last'保留最新值
    agg_rules = {
        col_name: 'last'  # 在周期转换时保留最新值
    }
    
    return df[[col_name]], agg_rules