import pandas as pd
import numpy as np

class MarketSentimentTiming:

    def __init__(self, price_df, amount_df, window=20, std_quantile=0.5, index_code=None):
        self.price_df = price_df
        self.amount_df = amount_df
        self.window = window
        self.std_quantile = std_quantile
        self.index_code = index_code

    def get_signal(self):
        if self.index_code and self.index_code in self.price_df.columns:
            price = self.price_df[self.index_code]
            amount = self.amount_df[self.index_code]
        else:
            price = self.price_df.mean(axis=1)
            amount = self.amount_df.mean(axis=1)
        momentum = price.pct_change(self.window).shift(-self.window)
        rolling_std = amount.rolling(self.window).std()
        std_threshold = rolling_std.quantile(self.std_quantile)
        signal = ((momentum > 0) & (rolling_std < std_threshold)).astype(int)
        signal = signal.reindex(self.price_df.index).fillna(method='ffill').fillna(0)
        return signal

# 框架自动调用的接口
def equity_signal(df, window=20, std_quantile=0.5, index_code=None):
    """
    df: DataFrame，index为日期，columns需包含'close'和'amount'（或类似字段）
    window, std_quantile, index_code: 择时参数
    返回：择时信号Series，index为日期，值为0或1
    """
    # 只保留数值型列，防止非数值数据导致报错
    numeric_df = df.select_dtypes(include=[np.number])
    if numeric_df.empty:
        raise ValueError("传入的df不包含任何数值型数据，无法计算择时信号。\n实际收到的df.head():\n" + str(df.head()))
    price_df = numeric_df
    amount_df = numeric_df
    timing = MarketSentimentTiming(price_df, amount_df, window, std_quantile, index_code)
    return timing.get_signal()