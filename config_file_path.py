from pathlib import Path

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
stock_data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-trading-data-pro"  # 参考：https://bbs.quantclass.cn/thread/39599
# (必选) 指数数据路径，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-main-index-data
index_data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-main-index-data"
# (可选) 财务数据，全量数据下载链接：https://www.quantclass.cn/data/stock/stock-fin-data-xbx
fin_data_path = r"/Users/wangminli/我的文档/Quant/邢不行量化课程-付费/下载数据/stock-fin-data-xbx"
