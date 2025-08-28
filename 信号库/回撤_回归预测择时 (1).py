import pandas as pd
import numpy as np
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    优化后的资金曲线择时策略：回归预测 + 双重回撤控制

    :param equity_df: 资金曲线DataFrame，需包含"净值"列
    :param args: 策略参数 (reg_window, forecast_days, dd_window, dd_threshold, max_dd_threshold)
        reg_window: 回归使用的历史窗口大小（默认60）
        forecast_days: 预测未来天数（默认5）
        dd_window: 回撤计算窗口大小（默认20）
        dd_threshold: 回撤触发减仓阈值（百分比，默认15%）
        max_dd_threshold: 最大可接受回撤阈值（百分比，默认25%）
    :return: 仓位信号Series (0.0~1.0)
    """
    # ===================== 参数设置（带默认值） =====================
    n = len(equity_df)
    reg_window = int(args[0]) if len(args) > 0 else 60
    forecast_days = int(args[1]) if len(args) > 1 else 5
    dd_window = int(args[2]) if len(args) > 2 else 20
    dd_threshold = float(args[3]) if len(args) > 3 else 0.15
    max_dd_threshold = float(args[4]) if len(args) > 4 else 0.25

    # ===================== 预计算公共指标 =====================
    # 初始化信号（默认全仓）
    signals = pd.Series(1.0, index=equity_df.index)

    # 1. 计算滚动最大值（用于回撤计算）
    rollmax = equity_df['净值'].rolling(dd_window, min_periods=1).max()
    drawdown = (rollmax - equity_df['净值']) / rollmax

    # 2. 计算波动率（用于动态阈值调整）
    daily_ret = equity_df['净值'].pct_change().fillna(0)
    volatility = daily_ret.rolling(dd_window).std() * np.sqrt(252)

    # 动态调整阈值（高波动市场使用更严格阈值）
    dynamic_dd_threshold = np.where(
        volatility > 0.25,
        dd_threshold * 0.85,  # 高波动时降低阈值
        dd_threshold
    )
    dynamic_max_dd_threshold = np.where(
        volatility > 0.25,
        max_dd_threshold * 0.85,  # 高波动时降低阈值
        max_dd_threshold
    )

    # ===================== 1. 回归预测风控 =====================
    # 向量化滚动回归预测（高效实现）
    X = sm.add_constant(np.arange(n))  # 添加常数项
    model = RollingOLS(equity_df['净值'], X, window=reg_window)
    rres = model.fit()

    # 预测未来回撤
    pred_drawdowns = pd.Series(0.0, index=equity_df.index)
    for i in range(reg_window, n - forecast_days):
        # 获取当前模型参数
        intercept, slope = rres.params.iloc[i, 0], rres.params.iloc[i, 1]

        # 预测未来路径
        future_idx = np.arange(i, i + forecast_days)
        future_pred = intercept + slope * future_idx

        # 计算预测回撤（从预测高点开始的最大回撤）
        peak = np.maximum.accumulate(future_pred)
        pred_dd = (peak[-1] - future_pred[-1]) / peak[-1]
        pred_drawdowns.iloc[i] = pred_dd

    # ===================== 2. 梯度化仓位控制体系 =====================
    # 按风险等级从高到低排序，建立仓位梯度
    signals = pd.Series(1.0, index=equity_df.index)  # 默认全仓

    # 梯度1：预测回撤超阈值 -> 减仓至80%
    pred_risk_condition = pred_drawdowns > dynamic_dd_threshold
    signals[pred_risk_condition] = 0.8

    # 梯度2：当前回撤超阈值 -> 减仓至60%
    current_risk_condition = drawdown > dynamic_dd_threshold
    signals[current_risk_condition] = 0.6

    # 梯度3：极端回撤风险 -> 空仓
    extreme_risk_condition = drawdown > dynamic_max_dd_threshold
    signals[extreme_risk_condition] = 0.0

    # 梯度4：双重风险叠加（当前回撤大且预测回撤大）-> 更严格减仓
    dual_risk_condition = current_risk_condition & pred_risk_condition
    signals[dual_risk_condition] = 0.4

    # ===================== 信号后处理 =====================
    # 1. 平滑处理（避免频繁切换）
    signals = signals.rolling(3, min_periods=1, center=True).mean()

    # 2. 确保仓位在[0,1]范围内
    signals = signals.clip(0.0, 1.0)

    # 3. 前向填充初始空值
    return signals.ffill().fillna(1.0)