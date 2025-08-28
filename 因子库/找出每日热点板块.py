import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from core.utils.path_kit import get_file_path
import os


def process_daily_data(date_group):
    """处理单个交易日的数据，计算热点行业"""
    date, group_data = date_group

    # 计算当日各行业的涨停数量
    industry_limit_up = group_data.groupby('新版申万一级行业名称')['是否涨停'].sum().reset_index()
    industry_limit_up = industry_limit_up.sort_values('是否涨停', ascending=False)

    # 获取前两个行业
    if len(industry_limit_up) >= 2:
        top_industry_1 = industry_limit_up.iloc[0]['新版申万一级行业名称']
        top_industry_2 = industry_limit_up.iloc[1]['新版申万一级行业名称']
    elif len(industry_limit_up) == 1:
        top_industry_1 = industry_limit_up.iloc[0]['新版申万一级行业名称']
        top_industry_2 = None
    else:
        top_industry_1 = None
        top_industry_2 = None

    return {
        '交易日期': date,
        '申万一级行业TOP1': top_industry_1,
        '申万一级行业TOP2': top_industry_2
    }


def identify_hot_industries_parallel():
    """
    使用并行处理识别热点板块并保存结果

    返回:
    hot_industries_df: 每日热点板块信息DataFrame
    """

    # 获取数据文件路径
    data_path = get_file_path("data", "运行缓存", "股票预处理数据.pkl")
    print(f"正在加载数据: {data_path}")

    # 读取数据
    try:
        candle_df_dict = pd.read_pickle(data_path)
        print(f"已加载 {len(candle_df_dict)} 只股票数据")
    except Exception as e:
        print(f"读取数据失败: {e}")
        return None

    # 将字典格式的数据转换为单个DataFrame
    all_stocks_data = []
    for stock_code, df in candle_df_dict.items():
        df['股票代码'] = stock_code
        all_stocks_data.append(df)

    stock_data = pd.concat(all_stocks_data, ignore_index=True)

    # 确保必要的列存在
    required_columns = ['股票代码', '交易日期', '收盘价', '涨停价', '新版申万一级行业名称']
    missing_columns = [col for col in required_columns if col not in stock_data.columns]
    if missing_columns:
        print(f"数据中缺少必要的列: {missing_columns}")
        return None

    # 转换日期格式并排序
    stock_data['交易日期'] = pd.to_datetime(stock_data['交易日期'])
    stock_data = stock_data.sort_values('交易日期')

    # 判断是否涨停：收盘价 >= 涨停价
    stock_data['是否涨停'] = (stock_data['收盘价'] >= stock_data['涨停价']).astype(int)

    # 按日期分组
    grouped_by_date = list(stock_data.groupby('交易日期'))

    # 使用并行处理每个交易日的数据
    hot_industries_data = []

    # 确定并行工作进程数（使用CPU核心数，但不超过日期数量）
    max_workers = min(os.cpu_count(), len(grouped_by_date))
    print(f"使用 {max_workers} 个并行进程处理 {len(grouped_by_date)} 个交易日的数据")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_date = {
            executor.submit(process_daily_data, date_group): date_group[0]
            for date_group in grouped_by_date
        }

        # 收集结果
        for future in as_completed(future_to_date):
            try:
                result = future.result()
                hot_industries_data.append(result)
            except Exception as e:
                date = future_to_date[future]
                print(f"处理日期 {date} 时出错: {e}")

    # 创建热点行业DataFrame并按日期排序
    hot_industries_df = pd.DataFrame(hot_industries_data)
    hot_industries_df = hot_industries_df.sort_values('交易日期')

    # 保存为CSV文件
    output_path = get_file_path("data", "运行缓存", "热点板块.csv")
    hot_industries_df.to_csv(output_path, index=False, encoding='GBK')
    print(f"热点板块数据已保存至: {output_path}")

    return hot_industries_df


# 示例使用
if __name__ == "__main__":
    # 识别热点板块
    hot_industries = identify_hot_industries_parallel()

    if hot_industries is not None:
        print("热点板块识别完成，最后10行数据:")
        print(hot_industries.tail(10))