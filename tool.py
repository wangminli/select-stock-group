from tools.tool2_策略查看器 import plot_stock_kline
from tools.tool1_因子分析 import factor_analysis
from core.model.backtest_config import load_config

if __name__ == '__main__':
    mode = input("请输入模式(1: 查看策略, 2: 查看因子): ")
    backtest_config = load_config()
    if mode == '1':
        plot_stock_kline(backtest_config, k_start='2009-01-01', k_end='2025-06-01', add_days=10)
    elif mode == '2':
        name = 'factor_20250801_24'
        factor_analysis(backtest_config, factor_name=name)
