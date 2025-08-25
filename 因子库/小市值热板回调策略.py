import pandas as pd

# 财务因子列：此列表用于存储财务因子相关的列名称
fin_cols = []  # 财务因子列，配置后系统会自动加载对应的财务数据

agg_rules = 'last'


def add_factor(df: pd.DataFrame, param=None, **kwargs) -> (pd.DataFrame, dict):
    # 1. 使用最近5天涨幅排序
    df['近5天涨幅'] = df["收盘价_复权"].pct_change(5)

    # 2. 将涨幅靠前的100只股票标记为1
    top_100 = df.nlargest(100, '近5天涨幅')

    # 3. 计算这100只股票在各行业的分布
    industry_counts = top_100['新版申万一级行业名称'].value_counts()

    # 4. 选取数量最多的2个行业
    top_industries = industry_counts.head(2).index.tolist()

    # 5. 将所有属于这两个行业的股票标记为1
    col_name = kwargs.get('col_name', '热门行业')
    df[col_name] = df['新版申万一级行业名称'].isin(top_industries).astype(int)

    # 定义因子聚合方式
    agg_rules = {col_name: 'last'}

    # 返回新计算的因子列以及因子聚合方式
    return df[[col_name]], agg_rules