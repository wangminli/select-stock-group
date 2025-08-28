import pandas as pd



def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    双均线趋势跟踪 回撤控制策略  均线买入 + 回撤卖出
    :param equity_df: 包含'净值'列的DataFrame
    :param args: [shortwindow, longwindow, drawdownthreshold]
    :return: 信号序列(1.0持有, 0.0空仓)
    """
    shortwindow, longwindow, drawdownthreshold = args



    # 计算均线
    equity_df['shortma'] = equity_df['净值'].rolling(shortwindow).mean()
    equity_df['longma'] = equity_df['净值'].rolling(longwindow).mean()



    # 初始化信号
    signals = pd.Series(0.0, index=equity_df.index)
    signals.iloc[0] = 1.0  # 默认持仓



    # 初始化变量
    position = 1.0  # 当前持仓状态(1.0持仓, 0.0空仓)
    peak = equity_df['净值'].iloc[0]  # 最高净值



    # 遍历生成信号
    for i in range(1, len(equity_df)):
        currentequity = equity_df['净值'].iloc[i]



        # 更新最高净值
        if position == 1.0:
            peak = max(peak, currentequity)



        # 计算当前回撤
        currentdrawdown = (peak - currentequity) / peak



        # 信号生成逻辑
        if position == 1.0:  # 当前持仓
            if currentdrawdown > drawdownthreshold:
                signals.iloc[i] = 0.0  # 回撤超阈值卖出
                position = 0.0
            else:
                signals.iloc[i] = 1.0  # 继续持仓
        else:  # 当前空仓
            if (equity_df['shortma'].iloc[i] > equity_df['longma'].iloc[i]) and \
                    (equity_df['shortma'].iloc[i - 1] <= equity_df['longma'].iloc[i - 1]):
                signals.iloc[i] = 1.0  # 均线上穿买入
                position = 1.0
                peak = currentequity  # 重置最高净值
            else:
                signals.iloc[i] = 0.0  # 继续空仓



    return signals




