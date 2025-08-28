"""
三力情绪引擎(TFI)择时因子 (参数化动态自适应版)

因子逻辑:
1. 攻击力(动态权重): 基于涨停股数量的市场进攻强度
   Attack = min(100, (当日涨停数 / max(1, N日涨停均数)) * 80

2. 防御力(动态权重): 基于跌停股换手率和超跌股占比的市场防御能力
   Defense = min(100, (跌停数 × 平均换手率 × 0.5 + 跌幅>M%个股占比 × 200)

3. 持续力(动态权重): 基于放量阳线个股占比的资金持续意愿
   Momentum = 放量阳线个股占比 × 100

核心参数:  市值30%、超跌阈值9%、放量阈值1.8
   其他参数还有-   - attack_window: 攻击力计算窗口 (默认10)
   - threshold_window: 动态阈值窗口 (默认30)
   - circuit_breaker: 熔断阈值 (默认7%)
   - weight_adjust_freq: 权重调整频率 ('M'月, 'Q'季)

仓位映射:
   TFI_smooth ≥ 动态上界: 80%-100%仓位
   动态中界 ≤ TFI_smooth < 动态上界: 50%-70%仓位
   动态下界 ≤ TFI_smooth < 动态中界: ≤30%仓位
   TFI_smooth < 动态下界: 空仓

应用场景:
   A股市场短线交易择时，适应不同风格市场
   预计10年数据处理时间: 约35秒 (i5-13400F)
"""

import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import os
import warnings
from core.model.backtest_config import load_config
from core.utils.path_kit import get_file_path

# 忽略警告
warnings.filterwarnings('ignore')


def calculate_attack(zt_count, ma_zt, scalar=80):
    """计算攻击力因子"""
    relative_strength = zt_count / max(1, ma_zt)
    return min(100, relative_strength * scalar)


def calculate_defense(dt_count, turnover, deep_fall_ratio,
                      turnover_weight=0.5, fall_weight=200):
    """计算防御力因子"""
    defense_value = (dt_count * turnover * turnover_weight) + (deep_fall_ratio * fall_weight)
    return min(100, max(0, defense_value))


def calculate_momentum(up_count, total_count, scalar=100):
    """计算持续力因子"""
    return min(100, (up_count / max(1, total_count)) * scalar)


def process_stock_data(df, volume_ratio, fall_threshold):
    """处理单只股票数据 (小市值为主)"""
    try:
        # 使用复权价格
        df = df.copy()

        # 计算关键指标
        df['涨跌幅_复权'] = df['收盘价_复权'].pct_change()
        df['涨停'] = (df['收盘价_复权'] >= df['涨停价']).astype(int)
        df['跌停'] = (df['收盘价_复权'] <= df['跌停价']).astype(int)

        # 成交量指标
        df['5日均量'] = df['成交量'].rolling(5, min_periods=1).mean()
        df['量比'] = df['成交量'] / df['5日均量'].replace(0, 1)

        # 使用参数化阈值
        df['放量阳线'] = (
                (df['涨跌幅_复权'] > 0.05) &  # 上涨阈值5% (适合小市值)
                (df['量比'] > volume_ratio)
        ).astype(int)

        df['跌幅超阈值'] = (df['涨跌幅_复权'] < fall_threshold).astype(int)

        return df[['交易日期', '股票代码', '涨停', '跌停', '放量阳线', '跌幅超阈值', '换手率', '总市值']]
    except Exception as e:
        print(f"处理股票数据出错: {e}")
        return None


# 新增函数：处理批次数据（解决lambda序列化问题）
def process_batch(batch, volume_ratio, fall_threshold):
    results = []
    for df in batch:
        res = process_stock_data(df, volume_ratio, fall_threshold)
        results.append(res)
    return results


def select_small_cap_stocks(all_stock_df, cap_percentile):
    """筛选小市值股票 (从小到大排序)"""
    if cap_percentile >= 1.0:
        return all_stock_df

    selected = []
    for date, group in all_stock_df.groupby('交易日期'):
        if '总市值' in group.columns:
            # 按市值从小到大排序 (小市值在前)
            group = group.sort_values('总市值', ascending=True)
            n = max(1, int(len(group) * cap_percentile))
            selected.append(group.head(n))  # 取市值最小的部分
        else:
            selected.append(group)

    return pd.concat(selected) if selected else all_stock_df


def calculate_dynamic_weights(daily_stats, current_date):
    """动态调整三力权重 (固定为月度调整)"""
    # 默认权重 (适合短线)
    weights = np.array([0.55, -0.25, 0.20])  # 提高攻击力权重

    # 检查是否有足够历史数据
    if len(daily_stats) < 20:  # 短线减少数据要求
        return weights

    # 回溯期 (固定为1个月)
    lookback = pd.DateOffset(months=1)
    start_date = current_date - lookback
    recent_data = daily_stats[daily_stats['交易日期'] >= start_date]

    if len(recent_data) < 5:  # 短线减少数据要求
        return weights

    # 计算市场波动率
    market_vol = recent_data['Attack'].std() + recent_data['Defense'].std() + recent_data['Momentum'].std()

    # 根据波动率调整权重
    if market_vol > 60:  # 高波动市场
        weights = np.array([0.45, -0.35, 0.20])
    elif market_vol < 25:  # 低波动市场
        weights = np.array([0.60, -0.15, 0.25])

    return weights


def calculate_dynamic_thresholds(tfi_values, window=30):
    """动态计算阈值 (窗口适合短线)"""
    if len(tfi_values) < 15:
        return 75, 55, 35  # 默认值

    window = min(window, len(tfi_values))
    recent_tfi = tfi_values[-window:]
    upper = np.percentile(recent_tfi, 75)
    middle = np.percentile(recent_tfi, 55)
    lower = np.percentile(recent_tfi, 35)

    # 约束阈值范围 (适合短线)
    upper = max(65, min(85, upper))
    lower = max(25, min(45, lower))
    middle = max(lower + 10, min(upper - 10, (upper + lower) / 2))

    return upper, middle, lower


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    # ==================== 参数解析 ====================
    # 保留3个关键参数：市值筛选百分比、超跌阈值、放量阈值
    # 默认值针对小市值短线交易优化
    params = {
        'cap_percentile': 0.3,  # 市值筛选百分比 (小市值为主) - 从小到大排序
        'fall_threshold': -0.09,  # 超跌阈值 (小市值波动大)
        'volume_ratio': 1.8,  # 放量阈值 (小市值放量要求高)
    }

    # 覆盖默认参数 (最多3个)
    param_keys = list(params.keys())
    for i, arg in enumerate(args):
        if i < len(param_keys):
            params[param_keys[i]] = arg

    # ==================== 数据准备 ====================
    # 确保日期格式正确
    equity_df = equity_df.copy()
    equity_df['交易日期'] = pd.to_datetime(equity_df['交易日期'])

    # 多进程配置
    workers = min(6, max(2, os.cpu_count() - 4))

    try:
        cache_path = get_file_path("data", "运行缓存", "股票预处理数据.pkl")
        all_candle_data_dict = pd.read_pickle(cache_path)
        stock_dfs = list(all_candle_data_dict.values())
    except Exception as e:
        print(f"数据加载失败: {e}")
        return pd.Series(0.5, index=equity_df.index)

    # 分批处理数据
    all_results = []
    batch_size = max(50, len(stock_dfs) // (workers * 10))

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = []
        for i in range(0, len(stock_dfs), batch_size):
            batch = stock_dfs[i:i + batch_size]
            # 使用新函数代替lambda
            futures.append(executor.submit(
                process_batch,  # 使用新定义的函数
                batch,
                params['volume_ratio'],
                params['fall_threshold']
            ))

        for future in tqdm(as_completed(futures), total=len(futures), desc="处理股票数据"):
            results = future.result()
            all_results.extend(results)  # 直接扩展结果列表

    # 过滤None值
    all_results = [r for r in all_results if r is not None]
    if not all_results:
        return pd.Series(0.5, index=equity_df.index)

    # 合并+筛选小市值股票
    all_stock_df = pd.concat(all_results, ignore_index=True)
    all_stock_df = select_small_cap_stocks(all_stock_df, params['cap_percentile'])

    # ==================== 因子计算 ====================
    # 聚合市场数据
    agg_funcs = {
        '涨停': 'sum',
        '跌停': 'sum',
        '放量阳线': 'sum',
        '跌幅超阈值': 'sum',
        '股票代码': 'count',
        '换手率': lambda x: x[all_stock_df['跌停'] == 1].mean()
    }
    daily_stats = all_stock_df.groupby('交易日期').agg(agg_funcs).rename(columns={
        '股票代码': '总股票数',
        '换手率': '跌停换手率',
        '跌幅超阈值': '跌幅超阈值数'
    }).reset_index()

    # 确保日期格式正确
    daily_stats['交易日期'] = pd.to_datetime(daily_stats['交易日期'])

    # 计算核心指标 (短线窗口)
    attack_window = 10  # 短线攻击力窗口 (可在此修改)
    daily_stats['N日涨停均数'] = daily_stats['涨停'].rolling(
        attack_window, min_periods=1).mean()
    daily_stats['超跌占比'] = daily_stats['跌幅超阈值数'] / daily_stats['总股票数']

    # 三力因子计算
    daily_stats['Attack'] = np.vectorize(calculate_attack)(
        daily_stats['涨停'], daily_stats['N日涨停均数']
    )
    daily_stats['Defense'] = np.vectorize(calculate_defense)(
        daily_stats['跌停'],
        daily_stats['跌停换手率'].fillna(0.20),  # 小市值换手率较高
        daily_stats['超跌占比']
    )
    daily_stats['Momentum'] = np.vectorize(calculate_momentum)(
        daily_stats['放量阳线'], daily_stats['总股票数']
    )

    # ==================== 动态适应 ====================
    # 动态权重调整 (适合短线)
    weights_history = []
    divisor = 10  # 每10天调整一次 (可在此修改调整频率)

    for idx, row in daily_stats.iterrows():
        current_date = row['交易日期']

        # 按频率调整
        if idx == 0 or (idx % divisor == 0):
            weights = calculate_dynamic_weights(daily_stats.iloc[:idx], current_date)
        else:
            weights = weights_history[-1] if weights_history else np.array([0.55, -0.25, 0.20])

        weights_history.append(weights)

    # 应用动态权重
    weights_array = np.array(weights_history)
    factors = daily_stats[['Attack', 'Defense', 'Momentum']].values
    daily_stats['TFI'] = np.sum(factors * weights_array, axis=1)

    # TFI平滑 (短线使用较短平滑)
    daily_stats['TFI_smooth'] = daily_stats['TFI'].ewm(span=2, adjust=False).mean()

    # 动态阈值 (窗口适合短线)
    tfi_values = daily_stats['TFI_smooth'].values
    dynamic_thresholds = []
    for i in range(len(tfi_values)):
        window_size = min(i + 1, 30)  # 30天窗口 (可在此修改)
        recent_tfi = tfi_values[max(0, i - window_size + 1):i + 1]
        thresholds = calculate_dynamic_thresholds(recent_tfi, window_size)
        dynamic_thresholds.append(thresholds)

    upper_b, middle_b, lower_b = zip(*dynamic_thresholds)

    # ==================== 仓位映射 ====================
    position = []
    for idx, row in daily_stats.iterrows():
        # 熔断机制 (小市值更敏感)
        if row['跌停'] / row['总股票数'] > 0.07:  # 7%跌停股比例
            position.append(0.0)
            continue

        # 动态仓位映射 (适合短线)
        tfi_val = row['TFI_smooth']
        if tfi_val >= upper_b[idx]:
            pos = 1.0  # 满仓
        elif tfi_val >= middle_b[idx]:
            pos = 0.9  # 90%仓位 (短线更激进)
        elif tfi_val >= lower_b[idx]:
            pos = 0.5  # 50%仓位
        else:
            pos = 0.0  # 空仓
        position.append(pos)

    daily_stats['position'] = position

    # ==================== 结果合并 ====================
    # 确保日期格式一致
    daily_stats['交易日期'] = pd.to_datetime(daily_stats['交易日期'])
    equity_df['交易日期'] = pd.to_datetime(equity_df['交易日期'])

    merged = pd.merge(
        equity_df[['交易日期']],
        daily_stats[['交易日期', 'position']],
        on='交易日期',
        how='left'
    ).ffill().fillna(0.5)

    return pd.Series(merged['position'].values, index=equity_df.index)