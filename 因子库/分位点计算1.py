import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime

# 设置pandas显示选项
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 50)
pd.set_option('expand_frame_repr', False)

# 获取数据
url = "https://indexapi.wind.com.cn/indicesWebsite/api/indexValuation?indexid=6644c422b6edae80b3c7a7d55803bc9e&limit=false&lan=cn"
response = requests.get(url)
data = response.json()

# 提取并转换数据
result_list = data['Result']
df = pd.DataFrame(result_list)
df['tradeDate'] = pd.to_datetime(df['tradeDate'], unit='ms').dt.strftime('%Y-%m-%d')
df['tradeDate'] = pd.to_datetime(df['tradeDate'])

# 调整列顺序和重命名
df = df[['tradeDate', 'close', 'peValue', 'pbValue', 'psValue', 'pcfValue', 'didValue']]
column_names = {
    'tradeDate': '交易日期',
    'close': '收盘价',
    'peValue': '市盈率',
    'pbValue': '市净率',
    'psValue': '市销率',
    'pcfValue': '市现率',
    'didValue': '股息率'
}
df = df.rename(columns=column_names)

# 合并交易日历
calendar = pd.read_csv(r'E:\game\框架\select-stock-3\data\交易日历.csv', encoding="utf-8", parse_dates=["交易日期"])
trading_dates = calendar["交易日期"]
calendar_df = pd.DataFrame(trading_dates)
calendar_df['交易日期'] = pd.to_datetime(calendar_df['交易日期'])
merged_df = pd.merge(calendar_df, df, on='交易日期', how='left')
merged_df = merged_df.ffill()
merged_df = merged_df[merged_df['交易日期'] >= '2016-01-01']


# 准确的分位点计算方法
def calculate_accurate_quantiles(df):
    # 确保按日期排序
    df = df.sort_values('交易日期')
    df['市净率'] = pd.to_numeric(df['市净率'], errors='coerce')

    # 计算交易日的年数偏移量
    trading_days_per_year = 250  # 大约每年250个交易日

    # 为每个日期创建索引
    df = df.reset_index(drop=True)

    # 计算3年分位点（约750个交易日）
    df['3年分位点'] = df.apply(
        lambda row: (df.loc[
                         (df['交易日期'] >= row['交易日期'] - pd.DateOffset(years=3)) &
                         (df['交易日期'] <= row['交易日期']), '市净率'
                     ] <= row['市净率']).mean() * 100, axis=1
    )

    # 计算5年分位点（约1250个交易日）
    df['5年分位点'] = df.apply(
        lambda row: (df.loc[
                         (df['交易日期'] >= row['交易日期'] - pd.DateOffset(years=5)) &
                         (df['交易日期'] <= row['交易日期']), '市净率'
                     ] <= row['市净率']).mean() * 100, axis=1
    )

    # 计算10年分位点（约2500个交易日）
    df['10年分位点'] = df.apply(
        lambda row: (df.loc[
                         (df['交易日期'] >= row['交易日期'] - pd.DateOffset(years=10)) &
                         (df['交易日期'] <= row['交易日期']), '市净率'
                     ] <= row['市净率']).mean() * 100, axis=1
    )

    # 保留一位小数
    df['3年分位点'] = df['3年分位点'].round(3)
    df['5年分位点'] = df['5年分位点'].round(3)
    df['10年分位点'] = df['10年分位点'].round(3)

    return df


# 应用分位点计算
merged_df = calculate_accurate_quantiles(merged_df)

# 保存结果
file_path = r'E:\game\data\micro_cap_index_pb\micro_cap_index_pb.csv'
merged_df.to_csv(file_path, index=False, mode='w', encoding='gbk')
print(f"微盘股历史pb及分位点数据已更新: {file_path}")
print(f"分位点数据已保留三位小数")