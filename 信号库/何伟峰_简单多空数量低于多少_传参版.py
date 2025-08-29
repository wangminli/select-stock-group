"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""

import pandas as pd
import numpy as np


def equity_signal(equity_df: pd.DataFrame, *args) -> pd.Series:
    """
    基于止损的择时信号（只有满仓或空仓两种状态）
    当资金曲线在近期跌幅超过阈值时，空仓
    其他情况，满仓
    
    :param equity_df: 资金曲线的DF
    :param args: 其他参数
        args[0]: 低于多少空仓择时
        args[1]: 止损阈值
    :return: 返回包含 leverage 的数据
    """
    低于多少空仓择时 = int(args[0])
    # ===== 获取策略参数
    # 定义特殊日期集合
    # 自动生成special_dates列表
    def generate_special_dates():
        """
        从策略信号结果文件中提取多空数量低于多少的日期
        
        返回:
            list: 符合条件的日期列表
        """
        # 文件路径
        file_path = r"G:\quantdata均线穷举\股票日线市值不限_均线遍历_1策略信号\结果文件\4.买入卖出_合计_信号及动能.CSV"
        # print(file_path)
        # exit()
        # 读取CSV文件
        try:
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(file_path, encoding='gb18030')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='gbk')  # 兼容主流中文编码格式
            
       
            # 筛选多空数量低于-80的记录
            filtered_df = df[df['【金叉数减死叉数】'] < 低于多少空仓择时]
            #我要计算筛选后的数据条数除以原来的数据条数
            print(f"【空仓日数除以整体交易日】：{filtered_df.shape[0] / df.shape[0]}")
           
            # 提取交易日期列并转换为列表
            dates_list = filtered_df['交易日期'].astype(str).tolist()
            # print(低于多少空仓择时)
            # print(dates_list)
            # exit()
            return dates_list
            
        except Exception as e:
            print(f"读取文件出错: {e}")
            return []
    
    special_dates = generate_special_dates()
    # print(special_dates)
    # exit()
    # 确保交易日期为datetime格式并转换为字符串
    equity_df['交易日期'] = pd.to_datetime(equity_df['交易日期'])
    date_str = equity_df['交易日期'].dt.strftime('%Y-%m-%d')

    # 判断日期是否在特殊日期集合中
    in_special_dates = date_str.isin(special_dates)

    # 初始化信号为1.0
    signals = pd.Series(1.0, index=equity_df.index)
    
    # 在特殊日期设置信号为0.0
    signals[in_special_dates] = 0.0

    
    return signals
