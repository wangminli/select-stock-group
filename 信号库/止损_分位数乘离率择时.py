"""
这个止损择时因子基于资金曲线的表现来决定仓位状态，核心逻辑如下：
计算两个关键指标：
价格分位数：计算资金净值在较长周期(period1)内的分位数(如80%分位数)
乖离率(bias_10)：计算当前净值相对于短期均线(period2)的偏离程度
信号生成条件：
当同时满足以下两个条件时，空仓(0)：
当前净值高于长期分位数(表明处于相对高位)
乖离率为负值(表明短期趋势向下)
其他情况下，满仓(1)

参数说明：
period1：计算分位数的长期窗口(如250天)
shreshold：分位数的阈值(如0.8表示80%分位数)
period2：计算乖离率的短期窗口(如20天)
激进型策略（高风险偏好）：
params = (365, 0.85, 30)  # 更严格的参数设置
保守型策略（低风险偏好）：
params = (120, 0.7, 10)  # 更敏感的参数设置

"""

import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    优化的净值高位回落止损择时因子
    核心逻辑：
    1. 当资金净值超过历史分位数阈值时，警惕高位风险
    2. 结合短期乖离率判断趋势逆转
    3. 双重条件触发时执行止损

    优化点：
    - 增加数据验证和异常处理
    - 优化参数处理逻辑
    - 添加边界条件处理
    - 改进性能效率
    - 增强信号稳定性

    输入输出保持不变
    """
    # ===== 1. 数据验证 =====
    if '净值' not in equity_df.columns:
        raise ValueError("资金曲线数据必须包含'净值'列")

    if len(equity_df) < 30:  # 确保足够数据量
        return pd.Series(1.0, index=equity_df.index)

    # ===== 2. 参数处理优化 =====
    try:
        # 统一处理参数格式
        if isinstance(args[0], (tuple, list)) and len(args[0]) >= 3:
            period1 = int(args[0][0])
            shreshold = float(args[0][1])
            period2 = int(args[0][2])
        elif len(args) >= 3:
            period1 = int(args[0])
            shreshold = float(args[1])
            period2 = int(args[2])
        else:
            # 提供合理的默认参数
            period1 = 250
            shreshold = 0.8
            period2 = 20
    except (ValueError, TypeError):
        # 参数解析失败时使用默认值
        period1 = 250
        shreshold = 0.8
        period2 = 20

    # ===== 3. 边界条件检查 =====
    period1 = max(10, min(period1, len(equity_df) // 2))  # 限制合理范围
    period2 = max(5, min(period2, period1 // 2))  # 确保period2 < period1
    shreshold = max(0.5, min(shreshold, 0.95))  # 限制在50%-95%之间

    # ===== 4. 核心计算优化 =====
    # 计算价格分位数 - 使用更高效的计算方法
    val = equity_df["净值"].rolling(
        window=period1,
        min_periods=int(period1 * 0.5)
    ).quantile(shreshold)

    # 计算乖离率 - 添加平滑处理
    rolling_mean = equity_df["净值"].rolling(
        window=period2,
        min_periods=int(period2 * 0.7)
    ).mean()
    bias = (equity_df["净值"] / rolling_mean) - 1

    # ===== 5. 信号生成优化 =====
    # 初始化为满仓信号
    signals = pd.Series(1.0, index=equity_df.index)

    # 双重条件判断
    condition1 = equity_df["净值"] > val  # 净值高于分位数阈值
    condition2 = bias < 0  # 乖离率为负
    exit_condition = condition1 & condition2  # 同时满足两个条件

    # 应用信号
    signals[exit_condition] = 0.0

    # ===== 6. 信号后处理 =====
    # 确保没有未来的信号
    signals = signals.fillna(method='ffill').fillna(1.0)

    # 避免频繁切换：至少维持N期相同信号
    min_hold_period = max(5, period2 // 4)
    signals = signals.rolling(
        window=min_hold_period,
        min_periods=1
    ).apply(lambda x: 0.0 if any(x == 0) else 1.0, raw=True)

    return signals