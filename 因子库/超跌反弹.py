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
    """
    计算超跌反弹因子

    工作流程：
    1. 计算指定周期内的价格跌幅
    2. 识别超跌后的反弹信号
    3. 根据反弹强度给出因子值

    :param df: pd.DataFrame，包含单只股票的K线数据
    :param param: 参数，格式为(drop_period, rebound_period, drop_threshold)
                  drop_period: 下跌计算周期，默认20
                  rebound_period: 反弹计算周期，默认5
                  drop_threshold: 超跌阈值，默认-0.2表示跌幅超过20%
    :param kwargs: 其他关键字参数，包括：
        - col_name: 新计算的因子列名
    :return: tuple
        - pd.DataFrame: 包含新计算的因子列
        - dict: 聚合方式字典
    """

    # ======================== 参数处理 ===========================
    col_name = kwargs['col_name']
    
    # 默认参数设置
    if param is None:
        drop_period = 20      # 默认下跌计算周期
        rebound_period = 5    # 默认反弹计算周期
        drop_threshold = -0.2 # 默认超跌阈值(-20%)
    elif isinstance(param, (list, tuple)) and len(param) >= 3:
        drop_period = int(param[0])
        rebound_period = int(param[1])
        drop_threshold = float(param[2])
    else:
        drop_period = 20
        rebound_period = 5
        drop_threshold = -0.2

    # ======================== 计算因子 ===========================
    # 初始化因子列
    df[col_name] = 0.0

    # 确保数据足够长
    if len(df) < drop_period + rebound_period:
        agg_rules = {col_name: 'last'}
        return df[[col_name]], agg_rules

    # 计算指定周期的涨跌幅
    df['drop_pct'] = df['收盘价_复权'].pct_change(drop_period)
    
    # 识别超跌信号（跌幅超过阈值）
    is_oversold = df['drop_pct'] < drop_threshold
    
    # 创建超跌信号标识
    df['oversold_signal'] = 0
    df.loc[is_oversold, 'oversold_signal'] = 1
    
    # 计算反弹幅度
    df['rebound_pct'] = df['收盘价_复权'].pct_change(rebound_period)
    
    # 遍历数据，寻找超跌反弹形态
    for i in range(drop_period, len(df) - rebound_period):
        # 如果当天是超跌日
        if df['oversold_signal'].iloc[i] == 1:
            oversold_idx = i
            oversold_drop = df['drop_pct'].iloc[i]
            
            # 检查随后的反弹情况
            rebound_start_idx = oversold_idx + 1
            rebound_end_idx = min(oversold_idx + rebound_period + 1, len(df))
            
            # 计算反弹期间的最大涨幅
            max_rebound = 0
            for j in range(rebound_start_idx, rebound_end_idx):
                if df['rebound_pct'].iloc[j] > max_rebound:
                    max_rebound = df['rebound_pct'].iloc[j]
            
            # 如果出现正向反弹
            if max_rebound > 0:
                # 计算超跌反弹强度
                # 强度由跌幅和反弹幅度共同决定
                # 跌幅越大，反弹越强，因子值越高
                oversold_strength = abs(oversold_drop)  # 超跌幅度
                rebound_strength = max_rebound         # 反弹幅度
                
                # 综合强度：跌幅越大且反弹越强，因子值越高
                combined_strength = (oversold_strength + rebound_strength) / 2
                
                # 标准化到0-1区间
                # 假设最大可能的强度为跌幅40%后反弹20%
                max_possible_strength = (0.4 + 0.2) / 2
                factor_value = min(1.0, combined_strength / max_possible_strength)
                
                # 为超跌日及其后rebound_period天分配因子值
                for j in range(0, min(rebound_period + 1, len(df) - oversold_idx)):
                    future_idx = oversold_idx + j
                    # 因子值随着时间递减
                    decay_factor = max(0, 1 - j / rebound_period)
                    df.iloc[future_idx, df.columns.get_loc(col_name)] = factor_value * decay_factor

    # 清理中间列
    df.drop(['drop_pct', 'oversold_signal', 'rebound_pct'], axis=1, errors='ignore', inplace=True)
    
    # 对因子值进行最终标准化处理，使其在0-1之间
    min_val = df[col_name].min()
    max_val = df[col_name].max()
    if max_val != min_val:
        df[col_name] = (df[col_name] - min_val) / (max_val - min_val)
    else:
        df[col_name] = 0.0

    # ======================== 聚合方式 ===========================
    agg_rules = {
        col_name: 'last'  # 在周期转换时保留最新值
    }

    # 返回新计算的因子列以及因子聚合方式
    return df[[col_name]], agg_rules
