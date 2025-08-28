import pandas as pd
import numpy as np
from core.utils.path_kit import get_file_path

# 财务因子列：此列表用于存储财务因子相关的列名称
fin_cols = []  # 财务因子列，配置后系统会自动加载对应的财务数据

# 全局变量，用于缓存热点板块数据
# hot_industry_cache = None


def load_hot_industry_data():
    """加载热点板块数据并缓存"""
    # global hot_industry_cache

    # 获取热点板块数据路径
    file_path = get_file_path("data", "运行缓存", "热点板块.csv")

    # 加载热点板块数据
    hot_industry_cache = pd.read_csv(
        file_path,
        encoding='gbk',
        parse_dates=['交易日期']
    )
    # print("热点板块数据加载成功")

    return hot_industry_cache


def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    """
    计算热点板块因子

    参数:
    df: 单个股票的K线数据
    param: 因子参数
    kwargs: 其他参数，包含col_name(因子列名称)

    返回:
    包含因子列的DataFrame和聚合规则字典
    """
    # ======================== 参数处理 ===========================
    # 从kwargs中提取因子列的名称
    col_name = kwargs['col_name']

    # 加载热点板块数据
    hot_industry_df = load_hot_industry_data()

    # 如果热点板块数据为空，直接返回全0的因子列
    if hot_industry_df.empty:
        df[col_name] = 0
        agg_rules = {col_name: 'last'}
        return df[[col_name]], agg_rules

    # ======================== 计算因子 ===========================
    # 确保df中有交易日期列
    if '交易日期' not in df.columns:
        print("错误: 输入数据中缺少'交易日期'列")
        df[col_name] = 0
        agg_rules = {col_name: 'last'}
        return df[[col_name]], agg_rules

    # 确保df中有新版申万一级行业名称列
    if '新版申万一级行业名称' not in df.columns:
        print("错误: 输入数据中缺少'新版申万一级行业名称'列")
        df[col_name] = 0
        agg_rules = {col_name: 'last'}
        return df[[col_name]], agg_rules

    # 合并热点板块数据

    # 创建df的副本，避免修改原始数据
    df_merged = df.copy()

    # 合并热点板块数据
    df_merged = pd.merge(
        df_merged,
        hot_industry_df,
        how='left',
        on='交易日期'
    )

    # 处理行业名称缺失值
    df_merged['新版申万一级行业名称'] = df_merged['新版申万一级行业名称'].ffill().bfill()
    df_merged['新版申万一级行业名称'] = df_merged['新版申万一级行业名称'].astype(str)

    # 处理热点行业TOP1和TOP2的缺失值
    df_merged['申万一级行业TOP1'] = df_merged['申万一级行业TOP1'].fillna('')
    df_merged['申万一级行业TOP2'] = df_merged['申万一级行业TOP2'].fillna('')
    df_merged['申万一级行业TOP1'] = df_merged['申万一级行业TOP1'].astype(str)
    df_merged['申万一级行业TOP2'] = df_merged['申万一级行业TOP2'].astype(str)

    # 初始化因子列为0
    df_merged[col_name] = 0

    # 判断是否属于热点板块
    # 条件1: 行业名称等于TOP1行业
    top1_condition = df_merged['新版申万一级行业名称'] == df_merged['申万一级行业TOP1']
    # 条件2: 行业名称等于TOP2行业
    top2_condition = df_merged['新版申万一级行业名称'] == df_merged['申万一级行业TOP2']

    # 满足任一条件则标记为热点板块
    df_merged.loc[top1_condition | top2_condition, col_name] = 1

    # 提取因子列
    result_df = df_merged[[col_name]]



    # ======================== 聚合方式 ===========================
    # 定义因子聚合方式
    agg_rules = {
        col_name: 'last'  # 在周期转换时保留该因子的最新值
    }

    # 返回新计算的因子列以及因子聚合方式
    return result_df, agg_rules