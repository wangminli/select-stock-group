"""市值敏感性：聚焦对市场情绪最敏感的小市值群体
双维验证：上涨比例（广度）+中位涨幅（强度）
动态适应：EWM平滑自动适应市场节奏
噪声过滤：流通市值过滤无效信号
# 牛市参数
牛市参数 = [0.3, 3, 0.52, 0.002]
# 震荡市参数
震荡市参数 = [0.2, 10, 0.58, 0.003]
# 熊市参数
熊市参数 = [0.15, 5, 0.62, 0.005]
"""

import pandas as pd
import numpy as np
import os
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from core.model.backtest_config import load_config
from core.utils.path_kit import get_file_path
from tqdm import tqdm

# 忽略警告
warnings.filterwarnings('ignore')


def calculate_stock_metrics(stock_df: pd.DataFrame) -> pd.DataFrame:
    """
    计算单只股票的关键指标（无未来函数）
    :param stock_df: 单只股票数据
    :return: 包含关键指标的DataFrame
    """
    try:
        # 过滤风险股票（ST/*ST/退市股/北交所股票）
        st_condition = stock_df['股票名称'].str.contains('ST|S|\\*|退', regex=True, na=False)
        bj_condition = stock_df['股票代码'].str.startswith(('8', 'bj'), na=False)
        valid_df = stock_df[~(st_condition | bj_condition)].copy()

        if valid_df.empty:
            return pd.DataFrame()

        # 确保日期格式正确
        valid_df['交易日期'] = pd.to_datetime(valid_df['交易日期'])

        # 计算指标
        valid_df['涨跌幅'] = valid_df['收盘价_复权'].pct_change()
        valid_df['是否上涨'] = (valid_df['涨跌幅'] > 0).astype(int)
        valid_df['是否下跌'] = (valid_df['涨跌幅'] < 0).astype(int)

        # 仅保留必要字段
        return valid_df[['交易日期', '股票代码', '涨跌幅', '是否上涨', '是否下跌', '流通市值']]

    except Exception as e:
        print(f"股票计算异常: {e}")
        return pd.DataFrame()


def process_stock_batch(stock_batch: list) -> list:
    """
    处理一批股票数据
    :param stock_batch: 股票数据批次
    :return: 处理结果列表
    """
    batch_results = []
    for df in stock_batch:
        result = calculate_stock_metrics(df)
        if not result.empty:
            batch_results.append(result)
    return batch_results


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    小市值情绪双引擎择时因子

    核心逻辑：
    1. 基于全市场股票表现计算市场情绪指标
    2. 使用两个关键维度：
       - 上涨股票比例（市场广度）
       - 中位数涨跌幅（市场强度）
    3. 动态平滑机制适应不同市场节奏

    参数说明：
    args[0]: 小市值分位阈值 (0-1)
    args[1]: 情绪平滑窗口 (交易日)
    args[2]: 上涨比例阈值 (0-1)
    args[3]: 中位涨幅阈值 (0-1)

    应用场景：
    - 适用于A股市场，特别擅长捕捉市场转折点
    - 建议用于中短线趋势跟踪策略
    - 需配合全市场预处理数据使用

    性能预估（10年A股数据）：
    - 数据加载：10-20秒
    - 并行计算：1-3分钟
    - 信号生成：<3秒
    - 总计：1.5-4分钟
    - 峰值内存：10-12GB
    - CPU峰值：60-70%
    """
    # ===== 1. 参数解析 =====
    cap_percentile = float(args[0])  # 小市值分位阈值
    smooth_window = int(args[1])  # 情绪平滑窗口
    up_ratio_thresh = float(args[2])  # 上涨比例阈值
    median_ret_thresh = float(args[3])  # 中位涨幅阈值

    # ===== 2. 数据准备 =====
    # 确定日期列并确保为datetime类型
    date_col = '交易日期' if '交易日期' in equity_df.columns else 'date'
    equity_df = equity_df.copy()
    equity_df[date_col] = pd.to_datetime(equity_df[date_col])
    start_date = equity_df[date_col].min()
    end_date = equity_df[date_col].max()

    # 加载预处理数据
    cache_path = get_file_path("data", "运行缓存", "股票预处理数据.pkl")
    if not os.path.exists(cache_path):
        print(f"❌ 预处理数据不存在: {cache_path}")
        return pd.Series(1.0, index=equity_df.index)  # 默认满仓

    try:
        all_candle_data_dict = pd.read_pickle(cache_path)
        print(f"✅ 成功加载预处理数据: {len(all_candle_data_dict)}只股票")
    except Exception as e:
        print(f"❌ 数据加载失败: {e}")
        return pd.Series(1.0, index=equity_df.index)

    # ===== 3. 并行计算市场指标 =====
    stock_dfs = list(all_candle_data_dict.values())
    market_data = []

    # 智能资源分配 (i5-13400F: 10核16线程)
    workers = min(8, (os.cpu_count() or 4) - 2)  # 保留2个核心
    batch_size = max(100, len(stock_dfs) // (workers * 10))  # 动态批次大小

    print(f"⚙️ 开始并行计算: {workers}进程, {batch_size}批次大小")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        # 创建批次任务
        futures = []
        for i in range(0, len(stock_dfs), batch_size):
            batch = stock_dfs[i:i + batch_size]
            futures.append(executor.submit(process_stock_batch, batch))

        # 使用tqdm显示进度条
        for future in tqdm(as_completed(futures), total=len(futures), desc="计算市场指标"):
            batch_results = future.result()
            if batch_results:
                market_data.extend(batch_results)

    if not market_data:
        print("⚠️ 无有效股票数据")
        return pd.Series(1.0, index=equity_df.index)

    # 合并数据
    all_stocks = pd.concat(market_data, ignore_index=True)
    print(f"📊 合并后数据量: {len(all_stocks)}行")

    # ===== 4. 小市值股票筛选 =====
    def filter_small_cap(group):
        if group.empty:
            return group
        cap_threshold = group['流通市值'].quantile(cap_percentile)
        return group[group['流通市值'] <= cap_threshold]

    small_cap = all_stocks.groupby('交易日期', group_keys=False).apply(filter_small_cap)

    if small_cap.empty:
        print("⚠️ 小市值筛选后无数据")
        return pd.Series(1.0, index=equity_df.index)

    print(f"🔍 小市值股票数量: {len(small_cap)}")

    # ===== 5. 计算市场情绪指标 =====
    daily_stats = small_cap.groupby('交易日期').agg(
        total_stocks=('股票代码', 'count'),
        up_stocks=('是否上涨', 'sum'),
        median_ret=('涨跌幅', 'median')
    ).reset_index()

    # 确保日期为datetime类型
    daily_stats['交易日期'] = pd.to_datetime(daily_stats['交易日期'])

    # 计算上涨比例
    daily_stats['up_ratio'] = daily_stats['up_stocks'] / daily_stats['total_stocks']

    # 指数平滑处理
    daily_stats['smoothed_ratio'] = daily_stats['up_ratio'].ewm(
        span=smooth_window, adjust=False, min_periods=1).mean()
    daily_stats['smoothed_median'] = daily_stats['median_ret'].ewm(
        span=smooth_window, adjust=False, min_periods=1).mean()

    # ===== 6. 生成交易信号 =====
    # 合并到资金曲线 (确保日期类型一致)
    merged = pd.merge(
        equity_df[[date_col]],
        daily_stats[['交易日期', 'smoothed_ratio', 'smoothed_median']],
        left_on=date_col,
        right_on='交易日期',
        how='left'
    )

    # 信号条件
    condition1 = merged['smoothed_ratio'] > up_ratio_thresh
    condition2 = merged['smoothed_median'] > median_ret_thresh

    # 信号生成（双条件满足）
    signals = pd.Series(1.0, index=merged.index)  # 默认满仓
    signals.loc[~(condition1 & condition2)] = 0.0  # 不满足条件时空仓

    # 向前填充缺失值
    signals = signals.ffill().fillna(1.0)

    print("✅ 信号生成完成")
    return signals