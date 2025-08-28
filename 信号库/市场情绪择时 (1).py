import pandas as pd
import numpy as np

from core.utils.path_kit import get_file_path
from typing import Dict, Tuple

from core.model.backtest_config import load_config

from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import statsmodels.api as sm


def merge_and_filter_stock(candle_df_dict: Dict[str, pd.DataFrame]):
    # 合并所有股票数据
    all_stock_df = pd.concat(
        candle_df_dict.values(),
        axis=0,
        ignore_index=True
    )

    # 获取每只股票的第一天数据（按交易日排序）
    first_trade = (
        all_stock_df.sort_values('交易日期').groupby('股票代码', as_index=False).first()[['股票代码', '股票名称', '交易日期']]
    )

    # 判断是否为 N 开头（即上市日）
    first_trade['上市日期'] = pd.NaT
    mask_n = first_trade['股票名称'].str.startswith('N')
    first_trade.loc[mask_n, '上市日期'] = first_trade.loc[mask_n, '交易日期']

    # 保留 股票代码 和 上市日期，回填到原数据中
    listing_date_map = first_trade.set_index('股票代码')['上市日期']
    all_stock_df['上市日期'] = all_stock_df['股票代码'].map(listing_date_map)

    # 执行剔除规则
    all_stock_df = all_stock_df[~all_stock_df['股票名称'].str.contains('ST|S|\\*|退', regex=True)]

    # 剔除论文中的未开板次新股
    all_stock_df = all_stock_df[
        ~(((all_stock_df['交易日期'] - all_stock_df['上市日期']).dt.days <= 10) & 
        (all_stock_df['一字涨停']))
    ]

    # 清理中间计算列
    all_stock_df = all_stock_df.drop(['上市日期'], axis=1, errors='ignore')

    return all_stock_df


def calculate_dz(all_stock_df: pd.DataFrame) -> pd.Series:
    """
    矢量化计算实际跌涨停比（DZ）
    - 使用收盘价 vs 涨停价/跌停价判断
    - 默认数据已剔除 ST 股、未开板次新股

    参数:
    all_stock_df (pd.DataFrame): 预处理后的股票数据（已剔除ST和次新一字板）

    返回:
    pd.Series: 每日实际跌涨停比（index为交易日期）
    """
    df = all_stock_df.copy()
    # df["交易日期"] = pd.to_datetime(df["交易日期"])

    # 判断是否涨停/跌停（矢量化）
    df["是否涨停"] = df["收盘价"] >= df["涨停价"]
    df["是否跌停"] = df["收盘价"] <= df["跌停价"]

    # 每日统计
    grouped = df.groupby("交易日期").agg(
        涨停数=("是否涨停", "sum"),
        跌停数=("是否跌停", "sum")
    )

    # 计算DZ
    grouped["DZ"] = grouped["跌停数"] / grouped["涨停数"].replace(0, np.nan)

    # 处理除数为0的情况，用99分位数填补
    p99 = grouped["DZ"].quantile(0.99)
    grouped["DZ"] = grouped["DZ"].fillna(p99)

    return grouped["DZ"]


def calculate_zd(all_stock_df: pd.DataFrame) -> pd.Series:
    """
    矢量化计算上涨下跌比（ZD）
    - 上涨下跌比 = 当日上涨家数 / 下跌家数
    - 假设数据已剔除 ST 和未开板次新股

    参数:
    all_stock_df (pd.DataFrame): 股票行情数据（含涨跌幅）

    返回:
    pd.Series: 每日上涨下跌比（交易日期为索引）
    """
    df = all_stock_df.copy()
    # df["交易日期"] = pd.to_datetime(df["交易日期"])

    # 标记是否上涨/下跌
    df["是否上涨"] = df["涨跌幅"] > 0
    df["是否下跌"] = df["涨跌幅"] < 0

    # 每日聚合统计
    grouped = df.groupby("交易日期").agg(
        上涨数=("是否上涨", "sum"),
        下跌数=("是否下跌", "sum")
    )

    # 计算上涨下跌比
    grouped["ZD"] = grouped["上涨数"] / grouped["下跌数"].replace(0, np.nan)

    # 用99分位数填充除数为0的情况
    p99 = grouped["ZD"].quantile(0.99)
    grouped["ZD"] = grouped["ZD"].fillna(p99)

    return grouped["ZD"]


def calculate_turn(all_stock_df: pd.DataFrame) -> pd.Series:
    """
    计算交易量(TURN)
    交易量为沪深两市当天成交总额除以沪深两市当天流通市值
    
    参数:
    all_stock_df (pd.DataFrame): 所有股票的数据
    
    返回:
    pd.Series: 按日期索引的交易量
    """
    # 按日期分组计算成交总额和流通总市值
    grouped = all_stock_df.groupby('交易日期').agg({
        '成交额': 'sum',
        '流通市值': 'sum'
    })
    
    # 计算交易量
    grouped['TURN'] = grouped['成交额'] / grouped['流通市值']
    
    return grouped['TURN']


def calculate_pb(all_stock_df: pd.DataFrame) -> pd.Series:
    """
    矢量化计算破板率（PB）：
    - PB = 当日破板数 / 当日曾触及涨停数
    - 默认数据已剔除ST股和未开板次新股

    参数:
    all_stock_df: pd.DataFrame，包含“最高价”、“收盘价”、“涨停价”

    返回:
    pd.Series: 每日破板率（index为交易日期）
    """
    df = all_stock_df.copy()
    df['交易日期'] = pd.to_datetime(df['交易日期'])

    # 标记“曾涨停”与“收盘涨停”
    df['曾涨停'] = df['最高价'] >= df['涨停价']
    df['收盘涨停'] = df['收盘价'] >= df['涨停价']

    # 破板 = 曾涨停 且 非收盘涨停
    df['破板'] = df['曾涨停'] & (~df['收盘涨停'])

    # 每日汇总
    grouped = df.groupby("交易日期").agg(
        曾涨停数=("曾涨停", "sum"),
        破板数=("破板", "sum")
    )

    # 破板率 = 破板 / 曾涨停
    grouped["PB"] = grouped["破板数"] / grouped["曾涨停数"].replace(0, np.nan)

    # 填补除数为0情况
    p99 = grouped["PB"].quantile(0.99)
    grouped["PB"] = grouped["PB"].fillna(p99)

    return grouped["PB"]


def calculate_consecutive_limit_up(all_stock_df: pd.DataFrame) -> pd.DataFrame:
    """
    矢量化方式计算每只股票的连续涨停天数（连板数）
    """
    df = all_stock_df.copy()
    df = df.sort_values(["股票代码", "交易日期"])
    df["收盘涨停"] = df["收盘价"] >= df["涨停价"]

    # 将非涨停置为 NaN，用于中断连续计数
    df["涨停标签"] = df["收盘涨停"].astype(int)
    df["涨停中断"] = df["涨停标签"].where(df["涨停标签"] == 1, np.nan)

    # groupby + transform + cumcount方式重构连板
    def calc_lb(group):
        return group["涨停中断"].groupby((group["涨停中断"].isna()).cumsum()).cumcount() + 1

    df["连板"] = df.groupby("股票代码").apply(calc_lb).reset_index(level=0, drop=True).fillna(0).astype(int)

    return df


def calculate_yesterday_performance(all_stock_df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    计算昨日破板（PBBX）、昨日涨停（ZTBX）、昨日连板（LBBX）的当日平均收益
    """

    df = all_stock_df.copy()
    df["交易日期"] = pd.to_datetime(df["交易日期"])
    df = df.sort_values(["股票代码", "交易日期"])

    # 衍生字段
    df["曾涨停"] = df["最高价"] >= df["涨停价"]
    df["收盘涨停"] = df["收盘价"] >= df["涨停价"]
    df["破板"] = df["曾涨停"] & (~df["收盘涨停"])

    # 连板提前计算
    df = calculate_consecutive_limit_up(df)

    # 获取昨日状态
    df["昨日_破板"] = df.groupby("股票代码")["破板"].shift(1)
    df["昨日_收盘涨停"] = df.groupby("股票代码")["收盘涨停"].shift(1)
    df["昨日_连板"] = df.groupby("股票代码")["连板"].shift(1)

    # 昨日破板的今日收益
    pbbx = df[df["昨日_破板"] == True].groupby("交易日期")["涨跌幅"].mean()

    # 昨日涨停的今日收益
    ztbx = df[df["昨日_收盘涨停"] == True].groupby("交易日期")["涨跌幅"].mean()

    # 昨日连板（>=2）的今日收益
    lbbx = df[df["昨日_连板"] >= 2].groupby("交易日期")["涨跌幅"].mean()

    # 命名
    return (
        pbbx.rename("PBBX"),
        ztbx.rename("ZTBX"),
        lbbx.rename("LBBX"),
    )


def calculate_height(all_stock_df: pd.DataFrame) -> pd.Series:
    """
    高度（GD）：每个交易日，市场中最大连板数（即单只股票连续收盘涨停最多的天数）
    
    参数:
    all_stock_df (pd.DataFrame): 股票行情数据，需包含“收盘价”、“涨停价”、“交易日期”、“股票代码”

    返回:
    pd.Series: 每日最高连板数（GD）
    """
    df = all_stock_df.copy()
    df["交易日期"] = pd.to_datetime(df["交易日期"])
    df = df.sort_values(["股票代码", "交易日期"])

    # 判断涨停
    df["收盘涨停"] = df["收盘价"] >= df["涨停价"]

    # 用 NaN 中断非涨停序列，用于计算连板
    df["涨停标签"] = df["收盘涨停"].astype(float)
    df["涨停中断"] = df["涨停标签"].where(df["收盘涨停"], np.nan)

    # 连板计算：每段连续涨停区域内递增（中断归0）
    df["连板"] = (
        df.groupby("股票代码")["涨停中断"]
          .transform(lambda x: x.groupby(x.isna().cumsum()).cumcount() + 1)
          .fillna(0).astype(int)
    )

    # 统计每日最大连板数
    gd = df.groupby("交易日期")["连板"].max()
    gd.name = "GD"

    return gd


# ===== 1. 标准化指标
def standardize_indicators(indicators_df: pd.DataFrame) -> pd.DataFrame:
    scaler = StandardScaler()
    scaled = scaler.fit_transform(indicators_df)
    return pd.DataFrame(scaled, columns=indicators_df.columns, index=indicators_df.index)


def perform_pca(standardized_df: pd.DataFrame, n_components=1) -> pd.Series:
    pca = PCA(n_components=n_components)
    isi_values = pca.fit_transform(standardized_df)
    isi_series = pd.Series(isi_values.flatten(), index=standardized_df.index, name='ISI')
    return isi_series


# ===== 2. 主成分加权构造 ISI（如论文）
def calculate_isi_paper_style(standardized_df: pd.DataFrame, n_components: int = 6) -> pd.Series:
    """
    按照论文公式（8）做主成分加权求和
    """
    pca = PCA(n_components=n_components)
    components = pca.fit_transform(standardized_df)  # shape: (T, n_components)

    # 主成分贡献率
    weights = pca.explained_variance_ratio_

    # 加权求和
    isi_values = components @ weights

    isi_series = pd.Series(isi_values, index=standardized_df.index, name='ISI')
    return isi_series


# ===== 3. 滞后回归分析
def lagged_regression_analysis(df: pd.DataFrame, isi_col='ISI', ret_col='收益率', max_lag=5) -> pd.DataFrame:
    """
    进行滞后性分析：检验 ISI_t 对未来 t+1~t+max_lag 的收益是否有预测能力
    
    参数:
    df : pd.DataFrame，包含列 ISI 和 收益率（必须先对齐好索引）
    isi_col : str，ISI 指标列名
    ret_col : str，收益率列名
    max_lag : int，最大滞后周期（默认 5）
    
    返回:
    pd.DataFrame，包含每个滞后期的 beta, t 值, p 值
    """
    results = []

    for k in range(1, max_lag + 1):
        df[f'{ret_col}_t+{k}'] = df[ret_col].shift(-k)

        # 去掉 NaN 行
        valid_df = df[[isi_col, f'{ret_col}_t+{k}']].dropna()

        # 构造 OLS 回归
        X = sm.add_constant(valid_df[isi_col])
        y = valid_df[f'{ret_col}_t+{k}']

        model = sm.OLS(y, X).fit()

        results.append({
            'lag': k,
            'beta': model.params[isi_col],
            't_value': model.tvalues[isi_col],
            'p_value': model.pvalues[isi_col],
            'n_obs': int(model.nobs)
        })

    result_df = pd.DataFrame(results)
    result_df.set_index('lag', inplace=True)

    return result_df


# ===== 4. ISI 分组未来收益分析
def isi_quantile_forward_returns(df: pd.DataFrame, isi_col="ISI", ret_col="收益率", quantiles=5, max_horizon=5) -> pd.DataFrame:
    """
    对 ISI 进行分组，并计算每组未来1~max_horizon日的平均收益
    """
    df = df.copy()
    df = df[[isi_col, ret_col]].dropna()

    # 生成未来收益列
    for k in range(1, max_horizon + 1):
        df[f"{ret_col}_t+{k}"] = df[ret_col].shift(-k)

    # 按 quantile 分组
    df["group"] = pd.qcut(df[isi_col], q=quantiles, labels=False)  # 0=最低，4=最高

    # 按组统计未来收益均值
    results = {}
    for k in range(1, max_horizon + 1):
        group_mean = df.groupby("group")[f"{ret_col}_t+{k}"].mean()
        results[f"t+{k}"] = group_mean

    return pd.DataFrame(results)


# ===== 5. 策略信号生成器（分位数映射）
def create_isi_signal_func(quantile_thresholds: dict, quantiles=5):
    def signal_func(isi_series: pd.Series) -> pd.Series:
        labels = list(range(quantiles))
        bins = pd.qcut(isi_series, q=quantiles, labels=labels)
        signal = bins.map(quantile_thresholds).astype(float)
        return signal
    return signal_func


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    backtest_config = load_config()
    # 读取指数数据（确保包含所有交易日）
    index_data = backtest_config.read_index_with_trading_date()

    # 读取所有股票的预处理数据
    candle_df_dict: Dict[str, pd.DataFrame] = pd.read_pickle(
        get_file_path('data', '运行缓存', '股票预处理数据.pkl')
    )
    all_stock_df = merge_and_filter_stock(candle_df_dict)
    all_stock_df['交易日期'] = pd.to_datetime(all_stock_df['交易日期'])

    # 计算各个指标
    dz = calculate_dz(all_stock_df)
    zd = calculate_zd(all_stock_df)
    turn = calculate_turn(all_stock_df)
    pb = calculate_pb(all_stock_df)
    pbbx, ztbx, lbbx = calculate_yesterday_performance(all_stock_df)
    gd = calculate_height(all_stock_df)

    # 创建指标DataFrame
    indicators_df = pd.DataFrame({
        'DZ': dz,
        'ZD': zd,
        'TURN': turn,
        'PB': pb,
        'PBBX': pbbx,
        'ZTBX': ztbx,
        'LBBX': lbbx,
        'GD': gd
    }).fillna(method='backfill')

    # 构建ISI（主成分分析）
    standardized_df = standardize_indicators(indicators_df)
    isi_series = calculate_isi_paper_style(standardized_df)

    # ==== 关键修改：对齐指数收益率 ====
    index_data['交易日期'] = pd.to_datetime(index_data['交易日期'])
    index_data.set_index('交易日期', inplace=True)
    aligned_index_ret = index_data['指数涨跌幅'].reindex(isi_series.index).ffill()

    df = pd.DataFrame({
        'ISI': isi_series,
        '收益率': aligned_index_ret
    })

    # 滞后回归分析（可选）
    # lagged_df = lagged_regression_analysis(df.copy(), ...)

    # 生成信号
    beta = 0.3
    span = 2 / (1 - beta) - 1
    isi_series_ema = isi_series.ewm(span=span).mean()

    quantile_map = {
        0: 1.0, 1: 0.8, 2: 0.2, 3: 0.0, 4: 1.0,
        5: 0.5, 6: 0.5, 7: 1.0, 8: 1.0, 9: 1.0
    }
    signal_func = create_isi_signal_func(quantile_map, quantiles=10)
    sentiment = signal_func(isi_series_ema)

    # 对齐到资金曲线日期
    start_date = equity_df['交易日期'].min()
    sentiment = sentiment[sentiment.index >= start_date]
    signals = equity_df['交易日期'].map(sentiment).fillna(1.0)

    return signals