"""
Log轴特别好看，一路笔直向上说明指数曲线非常棒！
RankIC也是一路向下，想对笔直
"""
"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""
import os
from pathlib import Path

# ====================================================================================================
# 1️⃣ 回测配置
# ====================================================================================================
# 回测数据的起始时间。如果因子使用滚动计算方法，在回测初期因子值可能为 NaN，实际的首次交易日期可能晚于这个起始时间。
start_date = "2009-01-01"
# 回测数据的结束时间。可以设为 None，表示使用最新数据；也可以指定具体日期，例如 '2024-11-01'。
end_date = None
# end_date = "2024-12-30"

# ====================================================================================================
# 2️⃣ 数据配置
# ====================================================================================================
# 数据中心的文件夹，使用数据客户端，并订阅相关数据，就不需要再手动指定每一个必要数据
data_center_path = Path(r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据")

# 数据源的定义，如果不使用数据客户端的时候，可以手动自定义以下逐个数据源
# ** 股票日线数据 和 指数数据是必须的，其他的数据可以在 data_sources 中定义 **
# (必选) 股票日线数据，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-trading-data
# 参考：https://bbs.quantclass.cn/thread/39599
# (必选) 股票日线数据，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-trading-data
stock_data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-trading-data-pro-2025-08-07"  # 参考：https://bbs.quantclass.cn/thread/39599
# (必选) 指数数据路径，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-main-index-data
index_data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-main-index-data-2025-08-06"
# (可选) 财务数据，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-fin-data-xbx
fin_data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-fin-data-xbx-2025-08-07"


# ====================================================================================================
# 3️⃣ 策略配置
# ====================================================================================================
strategy = {
    'name': '市值+资金流强度',
    'hold_period': '1W',
    'select_num': 10,
    "factor_list": [
        ('市值', True, None, 1),
        ('资金流强度', False, [5, 20, 1.2], 2),  # 新增因子

    ],
    "filter_list": [
        ('交易所', None, 'val:<=3'),  # 1沪A 2深A 3创业板 4科创板 5北交所
        ('月份', [1, 4], 'val:!=1'),  # 不在1、4月份选股
        ('市值', None, 'pct:<0.2'),
    ]}

days_listed = 250  # 上市至今交易天数
# excluded_boards = ["kcb", "bj"]  # 同时过滤创业板和科创板和北交所

# excluded_boards = ["bj"]  # 过滤板块，默认不过滤
excluded_boards = ["cyb", "kcb", "bj"]  # 同时过滤创业板和科创板和北交所


#equity_timing = {"name": "移动平均线", "params": [20]}
equity_timing = {"name": "MA双均线择时", "params": [10,20]}

# ====================================================================================================
# 4️⃣ 模拟交易配置
# 以下参数几乎不需要改动
# ====================================================================================================
initial_cash = 2_0000  # 初始资金10w
# initial_cash = 1_0000_0000  # 初始资金10w
# 手续费
c_rate = 0.86 / 10000
# 印花税
t_rate = 1 / 2000
# 并行运行的进程数
n_jobs = os.cpu_count() - 1

# =====参数预检查=====
if Path(stock_data_path).exists() is False:
    print(f"股票日线数据路径不存在：{stock_data_path}，请检查配置或联系助教，程序退出")
    exit()
if Path(index_data_path).exists() is False:
    print(f"指数数据路径不存在：{index_data_path}，请检查配置或联系助教，程序退出")
    exit()
