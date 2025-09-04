"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""
import os
from datetime import datetime
from pathlib import Path

# ====================================================================================================
# 1️⃣ 回测配置
# ====================================================================================================
# 回测数据的起始时间。如果因子使用滚动计算方法，在回测初期因子值可能为 NaN，实际的首次交易日期可能晚于这个起始时间。
start_date = "2008-10-09"
# 回测数据的结束时间。可以设为 None，表示使用最新数据；也可以指定具体日期，例如 '2024-11-01'。
end_date = "2025-06-10"

# ====================================================================================================
# 2️⃣ 数据配置
# ====================================================================================================
data_center_path = Path(r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据")

# 数据源的定义，如果不使用数据客户端的时候，可以手动自定义以下逐个数据源
# ** 股票日线数据 和 指数数据是必须的，其他的数据可以在 data_sources 中定义 **
# (必选) 股票日线数据，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-trading-data
# 参考：https://bbs.quantclass.cn/thread/39599
# (必选) 股票日线数据，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-trading-data
stock_data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-trading-data-pro"  # 参考：https://bbs.quantclass.cn/thread/39599
# (必选) 指数数据路径，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-main-index-data
index_data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-main-index-data"
# (可选) 财务数据，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-fin-data-xbx
fin_data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-fin-data-xbx"


# ====================================================================================================
# 3️⃣ 策略配置
# ====================================================================================================
strategy = {
    'name': f'组合筛选择时策略_{datetime.now().strftime("%Y%m%d_%H%M%S")}',  # 策略名
    'hold_period': '3D',  # 持仓周期，W 代表周，M 代表月
    'select_num': 7,  # 选股数量，可以是整数，也可以是小数，比如 0.1 表示选取 10% 的股票
    "factor_list": [  # 选股因子列表
        ('市值', True, None, 1),
        ('换手率', True, 5, 1),
        ('HRef', True, None, 1),
    ],
    "filter_list": [
        ("市值", None, f'pct:<0.1', True),
        ("收盘价", None, f'val:<15', True),
        ("收盘价", None, f'val:>3', True),
        ("月份", (1, 4), f'val:!=1', True),
        ("换手率", 4, f'pct:<0.95', True),
        ("换手率", 5, f'pct:<0.95', True),
        ("可用股票", None, f'val:==1', True),
        ("macd大小筛选", (12, 26, 9, 60), f'val:!=1', True),
        ("macd大小筛选", (6, 13, 5, 60), f'val:!=1', True),
        ("LRef", None, f'pct:>0.05', True),
        ("HRef", None, f'pct:<0.4', True),
        ("HRef", None, f'rank:>15', True),
    ] + [("WR筛选", i, f'val:==1', True) for i in range(10, 15)]
}

days_listed = 250  # 上市至今交易天数

#excluded_boards = ["bj"]

equity_timing = {'name': '动量择时信号', 'params': []}
excluded_boards = ["cyb", "kcb", "bj"]  # 同时过滤创业板和科创板和北交所


# =====性能优化设置=====
# 是否启用并行处理来加速计算过程
performance_settings = {
    "use_parallel": True,  # 设置为True启用并行处理，False为串行处理
    "max_workers": 8,   # 并行处理的最大工作进程数，None表示使用默认值(CPU核心数)
    "parallel_data_prep": True,  # 是否并行处理数据准备阶段(step1)
    "parallel_factor_calc": True,  # 是否并行处理因子计算阶段(step2)
    "chunk_size": 100,  # 每个工作进程处理的数据块大小，用于并行计算时的数据分割
}


# ====================================================================================================
# 4️⃣ 模拟交易配置
# 以下参数几乎不需要改动
# ====================================================================================================
initial_cash = 10_0000  # 初始资金10w
# initial_cash = 1_0000_0000  # 初始资金10w
# 手续费
c_rate = 1.2 / 10000
# 印花税
t_rate = 1 / 1000
# 并行运行的进程数
n_jobs = os.cpu_count() - 1

# =====参数预检查=====
if Path(stock_data_path).exists() is False:
    print(f"股票日线数据路径不存在：{stock_data_path}，请检查配置或联系助教，程序退出")
    exit()
if Path(index_data_path).exists() is False:
    print(f"指数数据路径不存在：{index_data_path}，请检查配置或联系助教，程序退出")
    exit()
