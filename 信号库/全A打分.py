""""【啊乐】全A打分因子择时_51.50%_-16.56%_3.11
 通过均线对所有股票进行判断，如果如果超过10%的股票处于20日均线的上方，判断市场处于上涨趋势。如果连10%都超不过，市场肯定处于下跌趋势。
计算10日总上涨家数和10日总下跌家数。如果10日累计上涨家数>10日累计下跌家数，判断市场处于上涨趋势。反之判断处于下跌趋势。
对所有股票成交量进行一个总和。上涨趋势往往伴随着量价齐升，下跌趋势成交量往往比较萎靡。这边使用30日均线进行判断，总成交量处于30日均线上方就是上涨趋势。
一个判断不够精细，三个总和起来去寻找垃圾行情，此择时方法是找出垃圾行情，把垃圾行情择时掉。只有打分>=2分的时候才进行开仓。 """

import pandas as pd
import os
from core.model.backtest_config import load_config
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed

warnings.filterwarnings('ignore')


def calculate_daily_metrics(stock_df, date_range, ma_window_for_stock):
    """计算单只股票的每日指标（无未来函数）"""
    try:
        df = stock_df.copy()
        # 过滤日期范围（使用当日数据计算当日指标）
        df = df[(df['交易日期'] >= date_range[0]) & (df['交易日期'] <= date_range[1])]
        if len(df) < ma_window_for_stock:  # 确保有足够数据计算均线
            return None

        # 排除ST等风险股票（使用当日状态）
        if '股票名称' in df.columns:
            # 只过滤ST类股票和北交所股票（不过滤科创板、创业板）
            st_condition = df['股票名称'].str.contains('ST|S|\\*|退', regex=True)
            # 过滤北交所股票（代码以8开头）
            if '股票代码' in df.columns:
                beijing_condition = df['股票代码'].str.startswith(('8', 'bj'))
            else:
                beijing_condition = False

            filter_condition = st_condition | beijing_condition
            df = df[~filter_condition]
            if df.empty:
                return None

        # 计算必要指标（仅使用当日及历史数据）
        df['涨跌幅'] = df['收盘价'].pct_change()

        # 使用历史数据计算均线（无未来函数）
        df[f'ma{ma_window_for_stock}'] = df['收盘价'].rolling(
            ma_window_for_stock, min_periods=1).mean()

        # 当日收盘价与当日计算的均线比较（无未来函数）
        df['在均线上方'] = (df['收盘价'] > df[f'ma{ma_window_for_stock}']).astype(int)

        # 确保有成交量列
        if '成交量' not in df.columns:
            df['成交量'] = 0

        # 保留总市值列用于后续筛选
        if '总市值' in df.columns:
            return df[['交易日期', '涨跌幅', '在均线上方', '成交量', '总市值']]
        else:
            return df[['交易日期', '涨跌幅', '在均线上方', '成交量']]
    except Exception as e:
        return None


def select_small_cap_stocks(all_data_df, cap_percentile):
    """
    按市值筛选股票：选择市值最小的前cap_percentile%股票
    """
    if '总市值' not in all_data_df.columns or cap_percentile >= 1.0:
        return all_data_df

    # 按交易日筛选小市值股票（使用当日总市值）
    selected_stocks = []
    for _, group in all_data_df.groupby('交易日期'):
        # 按市值排序（使用当日市值）
        group = group.sort_values('总市值')

        # 计算要保留的股票数量
        n = max(1, int(len(group) * cap_percentile))

        # 保留市值最小的前n只股票
        selected_stocks.append(group.head(n))

    return pd.concat(selected_stocks, ignore_index=True)


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    全A市场打分择时因子（优化版）
    基于市场宽度指标动态调整仓位:
    1. 均线上方股票比例(>=10%)
    2. 成交量突破均线
    3. 涨跌家数对比
    三者满足两项以上时开仓

    新增功能：
    - 按市值筛选：只分析市值最小的前N%股票
    - 可配置均线窗口
    - 无未来函数

    :param equity_df: 资金曲线DataFrame，需包含日期列
    :param args: 策略参数
        args[0]: 市值筛选百分比 (0.0-1.0)
        args[1]: 均线窗口 (用于计算每只股票的均线)
        args[2]: 涨跌家数滚动窗口
        args[3]: 成交量均线窗口
    :return: 仓位信号Series (1.0=满仓, 0.0=空仓)
    """
    # ===== 1. 参数设置 =====
    conf = load_config()
    cap_percentile = float(args[0])  # 市值筛选百分比
    ma_window_for_stock = int(args[1])  # 股票均线窗口
    updown_window = int(args[2])  # 涨跌家数滚动窗口
    volume_ma_window = int(args[3])  # 成交量均线窗口

    # ===== 2. 日期处理 =====
    # 确定日期列名称
    if 'date' in equity_df.columns:
        date_column = 'date'
    elif '交易日期' in equity_df.columns:
        date_column = '交易日期'
    else:
        equity_df = equity_df.reset_index()
        date_column = 'index'

    equity_df[date_column] = pd.to_datetime(equity_df[date_column])
    start_date = equity_df[date_column].min()
    end_date = equity_df[date_column].max()

    # ===== 3. 加载预处理数据 =====
    data_path = r'F:\BaiduNetdiskDownload\select-stock-3\data\运行缓存\股票预处理数据.pkl'

    try:
        all_candle_data_dict = pd.read_pickle(data_path)
    except Exception as e:
        # 返回默认信号（满仓）
        return pd.Series(1.0, index=equity_df.index)

    # ===== 4. 并行计算每日指标 =====
    all_data = []
    stock_dfs = list(all_candle_data_dict.values())

    # 使用ProcessPoolExecutor进行并行计算
    workers = max(1, os.cpu_count() - 2)  # 保留2个核心

    # 分批处理减少任务数量
    batch_size = max(1, len(stock_dfs) // (workers * 4))

    with ProcessPoolExecutor(max_workers=workers) as executor:
        # 创建任务（分批处理）
        futures = []
        for i in range(0, len(stock_dfs), batch_size):
            batch = stock_dfs[i:i + batch_size]
            futures.append(executor.submit(
                process_stock_batch,
                batch,
                (start_date, end_date),
                ma_window_for_stock
            ))

        # 收集结果
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_data.extend(result)

    # 检查是否有有效数据
    if not all_data:
        return pd.Series(1.0, index=equity_df.index)

    # 合并结果
    all_data_df = pd.concat(all_data, ignore_index=True)

    # ===== 5. 按市值筛选股票 =====
    if 0 < cap_percentile < 1.0:
        all_data_df = select_small_cap_stocks(all_data_df, cap_percentile)

    # ===== 6. 计算市场宽度指标 =====
    # 6.1 计算每日涨跌家数（使用当日数据）
    all_data_df['上涨'] = (all_data_df['涨跌幅'] > 0).astype(int)
    all_data_df['下跌'] = (all_data_df['涨跌幅'] < 0).astype(int)

    # 6.2 聚合每日指标（无未来函数）
    daily_stats = all_data_df.groupby('交易日期').agg(
        上涨股票数=('上涨', 'sum'),
        下跌股票数=('下跌', 'sum'),
        总股票数=('上涨', 'count'),
        总成交量=('成交量', 'sum'),
        均线上方股票数=('在均线上方', 'sum')
    ).reset_index()

    # 6.3 计算均线上方比例信号（使用当日数据）
    daily_stats['>=10%在上方'] = (
            daily_stats['均线上方股票数'] / daily_stats['总股票数'] >= 0.10
    ).astype(int)

    # 6.4 计算成交量信号（使用历史成交量）
    daily_stats['成交量均线'] = daily_stats['总成交量'].rolling(
        volume_ma_window, min_periods=1).mean()

    # 6.5 计算涨跌家数信号（使用滚动窗口历史数据）
    daily_stats['上涨滚动'] = daily_stats['上涨股票数'].rolling(
        updown_window, min_periods=1).sum()
    daily_stats['下跌滚动'] = daily_stats['下跌股票数'].rolling(
        updown_window, min_periods=1).sum()
    daily_stats['涨跌信号'] = (
            daily_stats['上涨滚动'] > daily_stats['下跌滚动']
    ).astype(int)

    # ===== 7. 合并到资金曲线 =====
    merged = pd.merge(
        equity_df[[date_column]],
        daily_stats[['交易日期', '>=10%在上方', '总成交量', '成交量均线', '涨跌信号']],
        left_on=date_column,
        right_on='交易日期',
        how='left'
    )

    # 填充缺失值（首日可能无数据）
    merged.fillna(0, inplace=True)

    # ===== 8. 生成交易信号 =====
    # 使用当日计算的指标生成信号（无未来函数）
    cond_ma = merged['>=10%在上方'] == 1
    cond_vol = merged['总成交量'] > merged['成交量均线']
    cond_ud = merged['涨跌信号'] == 1

    merged['满足数'] = cond_ma.astype(int) + cond_vol.astype(int) + cond_ud.astype(int)
    merged['信号'] = (merged['满足数'] >= 2).astype(float)

    # 返回与equity_df索引对齐的信号序列
    return pd.Series(merged['信号'].values, index=equity_df.index)


def process_stock_batch(stock_batch, date_range, ma_window_for_stock):
    """处理一批股票数据（减少任务数量）"""
    batch_results = []
    for stock_df in stock_batch:
        result = calculate_daily_metrics(stock_df, date_range, ma_window_for_stock)
        if result is not None:
            batch_results.append(result)
    return batch_results