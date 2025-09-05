'''
使用方法： 修改strategy_name即可
来源：https://bbs.quantclass.cn/thread/67977
'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
回测报告生成器

功能：
- 读取资金曲线和选股结果数据
- 生成交互式HTML报告
- 支持逐日查看回测过程
- 提供身临其境的实盘体验

作者：AI Assistant
创建时间：2024
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# 默认数据目录
strategy_name = "策略"
default_data_dir = r"/Users/wangminli/PycharmProjects/select-stock-group/data/回测结果/" + strategy_name

class BacktestReportGenerator:
    """回测报告生成器"""
    
    def __init__(self, data_dir: str):
        """
        初始化报告生成器
        
        Args:
            data_dir: 回测数据目录路径
        """
        self.data_dir = data_dir
        self.equity_data = None
        self.selection_data = None
        
    def load_data(self) -> bool:
        """
        加载回测数据
        
        Returns:
            bool: 是否成功加载数据
        """
        try:
            # 加载资金曲线数据
            equity_file = os.path.join(self.data_dir, '资金曲线.csv')
            if os.path.exists(equity_file):
                self.equity_data = pd.read_csv(equity_file, index_col=0)
                self.equity_data['交易日期'] = pd.to_datetime(self.equity_data['交易日期'])
                print(f"成功加载资金曲线数据，共 {len(self.equity_data)} 条记录")
            else:
                print(f"未找到资金曲线文件: {equity_file}")
                return False
                
            # 加载选股结果数据
            selection_file = os.path.join(self.data_dir, strategy_name + '选股结果.csv')
            if os.path.exists(selection_file):
                self.selection_data = pd.read_csv(selection_file)
                self.selection_data['交易日期'] = pd.to_datetime(self.selection_data['交易日期'])
                print(f"成功加载选股结果数据，共 {len(self.selection_data)} 条记录")
            else:
                print(f"未找到选股结果文件: {selection_file}")
                return False
                
            return True
            
        except Exception as e:
            print(f"加载数据时出错: {e}")
            return False
    
    def process_daily_data(self) -> Dict:
        """
        处理每日数据，生成用于HTML展示的数据结构
        
        Returns:
            Dict: 处理后的每日数据
        """
        daily_data = {}
        
        # 处理资金曲线数据
        for idx, row in self.equity_data.iterrows():
            date_str = row['交易日期'].strftime('%Y-%m-%d')
            
            daily_data[date_str] = {
                'date': date_str,
                'equity': {
                    '可用资金': float(row['账户可用资金']),
                    '持仓市值': float(row['持仓市值']),
                    '总资产': float(row['总资产']),
                    '净值': float(row['净值']),
                    '涨跌幅': float(row['涨跌幅']) if pd.notna(row['涨跌幅']) else 0.0,
                    '最大回撤': float(row['净值dd2here']),
                    #'杠杆': float(row['实际杠杆']),
                    '手续费': float(row['手续费'])
                },
                'holdings': []
            }
        
        # 处理选股结果数据
        for idx, row in self.selection_data.iterrows():
            date_str = row['交易日期'].strftime('%Y-%m-%d')
            
            if date_str in daily_data:
                holding_info = {
                    '股票代码': row['股票代码'],
                    '股票名称': row['股票名称'],
                    #'策略': row['策略'],
                    '目标资金占比': float(row['目标资金占比']),
                    #'择时信号': float(row['择时信号']),
                    #'选股因子排名': float(row['选股因子排名'])
                }
                daily_data[date_str]['holdings'].append(holding_info)
        
        return daily_data
    
    def generate_html_template(self) -> str:
        """
        生成HTML模板
        
        Returns:
            str: HTML模板字符串
        """
        return """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>回测报告 - 身临其境体验</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Microsoft YaHei', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
            color: white;
            padding: 5px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .header p {
            font-size: 1.2em;
            opacity: 0.9;
        }
        
        .controls {
            background: #f8f9fa;
            padding: 10px;
            border-bottom: 1px solid #e9ecef;
            display: flex;
            align-items: center;
            justify-content: space-between;
            flex-wrap: wrap;
            gap: 15px;
        }
        
        .date-selector {
            display: flex;
            align-items: center;
            gap: 15px;
        }
        
        .date-selector label {
            font-weight: bold;
            color: #495057;
        }
        
        .date-selector input, .date-selector select {
            padding: 8px 12px;
            border: 2px solid #dee2e6;
            border-radius: 8px;
            font-size: 14px;
            transition: border-color 0.3s;
        }
        
        .date-selector input:focus, .date-selector select:focus {
            outline: none;
            border-color: #007bff;
        }
        
        .nav-buttons {
            display: flex;
            gap: 10px;
        }
        
        .btn {
            padding: 10px 20px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
            font-weight: bold;
            transition: all 0.3s;
            text-decoration: none;
            display: inline-block;
        }
        
        .btn-primary {
            background: #007bff;
            color: white;
        }
        
        .btn-primary:hover {
            background: #0056b3;
            transform: translateY(-2px);
        }
        
        .btn-secondary {
            background: #6c757d;
            color: white;
        }
        
        .btn-secondary:hover {
            background: #545b62;
        }
        
        .content {
            padding: 30px;
        }
        
        .dashboard {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 30px;
            margin-bottom: 30px;
        }
        
        .card {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.08);
            border: 1px solid #e9ecef;
        }
        
        .card h3 {
            color: #2c3e50;
            margin-bottom: 20px;
            font-size: 1.4em;
            border-bottom: 2px solid #3498db;
            padding-bottom: 10px;
        }
        
        .metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 20px;
        }
        
        .metric {
            text-align: center;
            padding: 20px;
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            border-radius: 10px;
            border: 1px solid #dee2e6;
        }
        
        .metric-label {
            font-size: 0.9em;
            color: #6c757d;
            margin-bottom: 8px;
            font-weight: bold;
        }
        
        .metric-value {
            font-size: 1em;
            font-weight: bold;
            color: #2c3e50;
        }
        
        .metric-value.positive {
            color: #dc3545;
        }
        
        .metric-value.negative {
            color: #28a745;
        }
        
        .chart-container {
            height: 400px;
            margin-top: 20px;
        }
        
        .holdings-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        
        .holdings-table th,
        .holdings-table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #dee2e6;
        }
        
        .holdings-table th {
            background: #f8f9fa;
            font-weight: bold;
            color: #495057;
        }
        
        .holdings-table tr:hover {
            background: #f8f9fa;
        }
        
        .status-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 8px;
        }
        
        .status-active {
            background: #28a745;
        }
        
        .status-inactive {
            background: #6c757d;
        }
        
        .current-date {
            background: linear-gradient(135deg, #3498db 0%, #2980b9 100%);
            color: white;
            padding: 10px 25px;
            border-radius: 10px;
            font-size: 1.2em;
            font-weight: bold;
            text-align: center;
            margin-bottom: 20px;
            box-shadow: 0 5px 15px rgba(52, 152, 219, 0.3);
        }
        
        @media (max-width: 768px) {
            .dashboard {
                grid-template-columns: 1fr;
            }
            
            .controls {
                flex-direction: column;
                align-items: stretch;
            }
            
            .date-selector {
                justify-content: center;
            }
            
            .nav-buttons {
                justify-content: center;
            }
        }
        
        .loading {
            text-align: center;
            padding: 50px;
            color: #6c757d;
        }
        
        .error {
            background: #f8d7da;
            color: #721c24;
            padding: 15px;
            border-radius: 8px;
            margin: 20px 0;
            border: 1px solid #f5c6cb;
        }
        
        @keyframes bounce {
            0%, 20%, 50%, 80%, 100% {
                transform: translateY(0);
            }
            40% {
                transform: translateY(-10px);
            }
            60% {
                transform: translateY(-5px);
            }
        }
        
        @keyframes sparkle {
            0%, 100% {
                opacity: 0.3;
                transform: scale(0.8);
            }
            50% {
                opacity: 1;
                transform: scale(1.2);
            }
        }
        
        /* 天气效果动画 */
        .weather-container {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            pointer-events: none;
            overflow: hidden;
            z-index: 1000;
            transition: all 0.5s ease;
        }
        
        /* 天气背景效果 */
        .weather-bg-sunny {
            background: linear-gradient(135deg, #FFE066 0%, #FF6B35 100%);
            opacity: 0.1;
        }
        
        .weather-bg-cloudy {
            background: linear-gradient(135deg, #BDC3C7 0%, #2C3E50 100%);
            opacity: 0.15;
        }
        
        .weather-bg-rainy {
            background: linear-gradient(135deg, #4A90E2 0%, #2C3E50 100%);
            opacity: 0.2;
        }
        
        .weather-bg-snowy {
            background: linear-gradient(135deg, #E8F4FD 0%, #B8C6DB 100%);
            opacity: 0.25;
        }
        
        .weather-bg-stormy {
            background: linear-gradient(135deg, #2C3E50 0%, #000000 100%);
            opacity: 0.3;
        }
        
        /* 阳光效果 */
        .sunshine {
            position: absolute;
            top: 30px;
            right: 30px;
            width: 120px;
            height: 120px;
            background: radial-gradient(circle, #FFD700 20%, #FFA500 40%, transparent 70%);
            border-radius: 50%;
            animation: sunshine 4s ease-in-out infinite;
            box-shadow: 0 0 50px rgba(255, 215, 0, 0.6);
        }
        
        .sunshine::before {
            content: '';
            position: absolute;
            top: -20px;
            left: -20px;
            right: -20px;
            bottom: -20px;
            background: radial-gradient(circle, rgba(255, 215, 0, 0.4) 0%, transparent 70%);
            border-radius: 50%;
            animation: sunGlow 3s ease-in-out infinite alternate;
        }
        
        .sunshine::after {
            content: '';
            position: absolute;
            top: 50%;
            left: 50%;
            width: 200px;
            height: 200px;
            margin: -100px 0 0 -100px;
            background: conic-gradient(from 0deg, transparent, rgba(255, 215, 0, 0.3), transparent, rgba(255, 215, 0, 0.3), transparent);
            border-radius: 50%;
            animation: sunRays 8s linear infinite;
        }
        
        @keyframes sunshine {
            0%, 100% { transform: rotate(0deg) scale(1); }
            50% { transform: rotate(180deg) scale(1.2); }
        }
        
        @keyframes sunGlow {
            0% { opacity: 0.4; transform: scale(1); }
            100% { opacity: 0.8; transform: scale(1.5); }
        }
        
        @keyframes sunRays {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        /* 雨滴效果 */
        .rain {
            position: absolute;
            width: 4px;
            height: 40px;
            background: linear-gradient(to bottom, rgba(74, 144, 226, 0.8), #4A90E2, rgba(74, 144, 226, 0.3));
            border-radius: 2px;
            animation: rain-fall linear infinite;
            box-shadow: 0 0 6px rgba(74, 144, 226, 0.5);
        }
        
        .rain-heavy {
            width: 6px;
            height: 60px;
            background: linear-gradient(to bottom, rgba(30, 60, 114, 0.9), #1E3C72, rgba(30, 60, 114, 0.4));
            box-shadow: 0 0 10px rgba(30, 60, 114, 0.7);
        }
        
        @keyframes rain-fall {
            0% {
                transform: translateY(-100vh) rotate(10deg);
                opacity: 0.8;
            }
            100% {
                transform: translateY(100vh) rotate(10deg);
                opacity: 0;
            }
        }
        
        /* 雪花效果 */
        .snow {
            position: absolute;
            color: #fff;
            font-size: 1.5em;
            text-shadow: 0 0 10px rgba(255, 255, 255, 0.8);
            animation: snow-fall linear infinite;
        }
        
        .snow-large {
            font-size: 2.5em;
            text-shadow: 0 0 15px rgba(255, 255, 255, 0.9);
        }
        
        @keyframes snow-fall {
            0% {
                transform: translateY(-100vh) rotate(0deg);
                opacity: 0.9;
            }
            50% {
                opacity: 1;
            }
            100% {
                transform: translateY(100vh) rotate(360deg);
                opacity: 0;
            }
        }
        
        /* 云朵效果 */
        .cloud {
            position: absolute;
            background: #D3D3D3;
            border-radius: 50px;
            opacity: 0.7;
            animation: cloud-move 25s linear infinite;
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
        }
        
        .cloud::before,
        .cloud::after {
            content: '';
            position: absolute;
            background: #D3D3D3;
            border-radius: 50px;
        }
        
        .cloud-small {
            width: 80px;
            height: 35px;
        }
        
        .cloud-small::before {
            width: 40px;
            height: 40px;
            top: -20px;
            left: 15px;
        }
        
        .cloud-small::after {
            width: 50px;
            height: 30px;
            top: -15px;
            right: 12px;
        }
        
        .cloud-large {
            width: 120px;
            height: 50px;
            background: #A9A9A9;
        }
        
        .cloud-large::before {
            width: 60px;
            height: 60px;
            top: -30px;
            left: 20px;
            background: #A9A9A9;
        }
        
        .cloud-large::after {
            width: 70px;
            height: 45px;
            top: -22px;
            right: 18px;
            background: #A9A9A9;
        }
        
        @keyframes cloud-move {
            0% { transform: translateX(-150px); }
            100% { transform: translateX(calc(100vw + 150px)); }
        }
        
        /* 闪电效果 */
        .lightning {
            position: absolute;
            top: 20%;
            left: 50%;
            width: 8px;
            height: 200px;
            background: linear-gradient(to bottom, #FFD700, #FFA500, #FF6B35, transparent);
            transform: translateX(-50%);
            animation: lightning-flash 0.3s ease-in-out;
            opacity: 0;
            box-shadow: 0 0 20px #FFD700, 0 0 40px #FFA500;
            border-radius: 4px;
        }
        
        .lightning::before {
            content: '';
            position: absolute;
            top: 0;
            left: -4px;
            width: 16px;
            height: 200px;
            background: linear-gradient(to bottom, rgba(255, 215, 0, 0.6), rgba(255, 165, 0, 0.4), transparent);
            border-radius: 8px;
        }
        
        .lightning-branch {
            position: absolute;
            width: 4px;
            height: 80px;
            background: linear-gradient(to bottom, #FFD700, transparent);
            border-radius: 2px;
            animation: lightning-flash 0.3s ease-in-out;
            opacity: 0;
        }
        
        @keyframes lightning-flash {
            0%, 100% { opacity: 0; }
            20%, 80% { opacity: 1; }
            50% { opacity: 0.7; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="weather-container" id="weatherContainer"></div>
        <div class="header">
            <h1>📈 回测报告</h1>
            <p>身临其境体验每一个交易日</p>
        </div>
        
        <div class="controls">
            <div class="date-selector">
                <label for="dateInput">选择日期:</label>
                <input type="date" id="dateInput" onchange="changeDate()">
                <select id="quickSelect" onchange="quickSelectDate()">
                    <option value="">快速选择</option>
                    <option value="first">第一天</option>
                    <option value="last">最后一天</option>
                    <option value="max">净值最高点</option>
                    <option value="min">最大回撤点</option>
                </select>
            </div>
            
            <div class="nav-buttons">
                <button class="btn btn-secondary" onclick="previousDay()">⬅️ 上一天</button>
                <button class="btn btn-primary" id="playBtn" onclick="playAnimation()">▶️ 自动播放</button>
                <button class="btn btn-secondary" onclick="nextDay()">下一天 ➡️</button>
                <select id="speedSelect" onchange="changeSpeed()" style="margin-left: 10px; padding: 8px; border-radius: 5px; border: 1px solid #ccc;">
                    <option value="0.5">0.5x</option>
                    <option value="1" selected>1x</option>
                    <option value="2">2x</option>
                    <option value="4">4x</option>
                    <option value="8">8x</option>
                </select>
            </div>
        </div>
        
        <div class="content">
            <div class="current-date" id="currentDate">请选择日期</div>
            
            
            
            <div class="dashboard">
                <div class="card">
                    <h3>😊 今日心情</h3>
                    <div style="font-size: 3em; margin: 10px 0; animation: bounce 2s infinite;" id="moodEmoji">😊</div>
                    <div style="font-size: 1.2em; color: #666; font-weight: bold;" id="moodText">等待数据加载...</div>
                </div>
                <div class="card">
                    <h3>📊 资金状况</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">总资产</div>
                            <div class="metric-value" id="totalAssets">-</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">可用资金</div>
                            <div class="metric-value" id="availableFunds">-</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">持仓市值</div>
                            <div class="metric-value" id="holdingValue">-</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">净值</div>
                            <div class="metric-value" id="netValue">-</div>
                        </div>
                    </div>
                </div>
                
                <div class="card">
                    <h3>📈 收益指标</h3>
                    <div class="metrics">
                        <div class="metric">
                            <div class="metric-label">当日涨跌幅</div>
                            <div class="metric-value" id="dailyReturn">-</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">最大回撤</div>
                            <div class="metric-value" id="maxDrawdown">-</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">杠杆率</div>
                            <div class="metric-value" id="leverage">-</div>
                        </div>
                        <div class="metric">
                            <div class="metric-label">手续费</div>
                            <div class="metric-value" id="fees">-</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div class="card">
                <h3>📈 净值曲线</h3>
                <div class="chart-container">
                    <canvas id="equityChart"></canvas>
                </div>
            </div>
            
            <div class="card">
                <h3>🎯 当日持仓</h3>
                <div id="holdingsContainer">
                    <table class="holdings-table" id="holdingsTable">
                        <thead>
                            <tr>
                                <th>状态</th>
                                <th>股票代码</th>
                                <th>股票名称</th>
                                <th>策略</th>
                                <th>目标占比</th>
                                <th>择时信号</th>
                                <th>排名</th>
                            </tr>
                        </thead>
                        <tbody id="holdingsBody">
                            <tr>
                                <td colspan="7" style="text-align: center; color: #6c757d;">请选择日期查看持仓</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // 全局变量
        let backtestData = {};
        let currentDate = null;
        let dateList = [];
        let equityChart = null;
        let isPlaying = false;
        let playSpeed = 1;
        let playInterval = null;
        
        // 初始化数据
        function initData() {
            backtestData = {BACKTEST_DATA};
            dateList = Object.keys(backtestData).sort();
            
            if (dateList.length > 0) {
                // 设置日期选择器的范围
                const dateInput = document.getElementById('dateInput');
                dateInput.min = dateList[0];
                dateInput.max = dateList[dateList.length - 1];
                
                // 默认显示最后一天
                currentDate = dateList[dateList.length - 1];
                dateInput.value = currentDate;
                
                updateDisplay();
                initChart();
            }
        }
        
        // 更新显示
        function updateDisplay() {
            if (!currentDate || !backtestData[currentDate]) {
                return;
            }
            
            const data = backtestData[currentDate];
            const equity = data.equity;
            
            // 更新当前日期显示
            document.getElementById('currentDate').textContent = 
                `📅 ${currentDate} (第 ${dateList.indexOf(currentDate) + 1} 天)`;
            
            // 更新资金指标
            document.getElementById('totalAssets').textContent = 
                formatNumber(equity.总资产) + ' 元';
            document.getElementById('availableFunds').textContent = 
                formatNumber(equity.可用资金) + ' 元';
            document.getElementById('holdingValue').textContent = 
                formatNumber(equity.持仓市值) + ' 元';
            document.getElementById('netValue').textContent = 
                equity.净值.toFixed(4);
            
            // 更新收益指标
            const dailyReturnElement = document.getElementById('dailyReturn');
            const dailyReturn = (equity.涨跌幅 * 100).toFixed(2) + '%';
            dailyReturnElement.textContent = dailyReturn;
            dailyReturnElement.className = 'metric-value ' + 
                (equity.涨跌幅 >= 0 ? 'positive' : 'negative');
            
            const maxDrawdownElement = document.getElementById('maxDrawdown');
            const maxDrawdown = (equity.最大回撤 * 100).toFixed(2) + '%';
            maxDrawdownElement.textContent = maxDrawdown;
            maxDrawdownElement.className = 'metric-value negative';
            
            document.getElementById('leverage').textContent = 
                (equity.杠杆 * 100).toFixed(1) + '%';
            document.getElementById('fees').textContent = 
                formatNumber(equity.手续费) + ' 元';
            
            // 更新持仓表格
            updateHoldingsTable(data.holdings);
            
            // 更新心情模块
            updateMood(equity.涨跌幅, equity.最大回撤);
            
            // 更新图表
            updateChart();
        }
        
        // 更新持仓表格
        function updateHoldingsTable(holdings) {
            const tbody = document.getElementById('holdingsBody');
            
            if (holdings.length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" style="text-align: center; color: #6c757d;">当日无持仓</td></tr>';
                return;
            }
            
            tbody.innerHTML = holdings.map(holding => {
                const isActive = holding.目标资金占比 > 0;
                const statusClass = isActive ? 'status-active' : 'status-inactive';
                const statusText = isActive ? '持仓' : '观望';
                
                return `
                    <tr>
                        <td><span class="status-indicator ${statusClass}"></span>${statusText}</td>
                        <td>${holding.股票代码}</td>
                        <td>${holding.股票名称}</td>
                        <td>${holding.策略}</td>
                        <td>${(holding.目标资金占比 * 100)}%</td>
                        <td>${holding.择时信号}</td>
                        <td>${holding.选股因子排名}</td>
                    </tr>
                `;
            }).join('');
        }
        
        // 初始化图表
        function initChart() {
            const ctx = document.getElementById('equityChart').getContext('2d');
            
            equityChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [{
                        label: '净值曲线',
                        data: [],
                        borderColor: '#3498db',
                        backgroundColor: 'rgba(52, 152, 219, 0.1)',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.1
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: false,
                            title: {
                                display: true,
                                text: '净值'
                            }
                        },
                        x: {
                            title: {
                                display: true,
                                text: '日期'
                            }
                        }
                    },
                    plugins: {
                        legend: {
                            display: true,
                            position: 'top'
                        },
                        tooltip: {
                            mode: 'index',
                            intersect: false
                        }
                    },
                    interaction: {
                        mode: 'nearest',
                        axis: 'x',
                        intersect: false
                    }
                }
            });
            
            updateChart();
        }
        
        // 更新图表
        function updateChart() {
            if (!equityChart) return;
            
            const currentIndex = dateList.indexOf(currentDate);
            const chartDates = dateList.slice(0, currentIndex + 1);
            const chartData = chartDates.map(date => backtestData[date].equity.净值);
            
            equityChart.data.labels = chartDates;
            equityChart.data.datasets[0].data = chartData;
            equityChart.update('none');
        }
        
        // 格式化数字
        function formatNumber(num) {
            return new Intl.NumberFormat('zh-CN', {
                minimumFractionDigits: 0,
                maximumFractionDigits: 0
            }).format(num);
        }
        
        // 日期变化
        function changeDate() {
            const dateInput = document.getElementById('dateInput');
            const selectedDate = dateInput.value;
            
            if (selectedDate && backtestData[selectedDate]) {
                currentDate = selectedDate;
                updateDisplay();
            }
        }
        
        // 快速选择日期
        function quickSelectDate() {
            const select = document.getElementById('quickSelect');
            const value = select.value;
            
            if (!value) return;
            
            let targetDate = null;
            
            switch (value) {
                case 'first':
                    targetDate = dateList[0];
                    break;
                case 'last':
                    targetDate = dateList[dateList.length - 1];
                    break;
                case 'max':
                    let maxNetValue = 0;
                    dateList.forEach(date => {
                        const netValue = backtestData[date].equity.净值;
                        if (netValue > maxNetValue) {
                            maxNetValue = netValue;
                            targetDate = date;
                        }
                    });
                    break;
                case 'min':
                    let minDrawdown = 0;
                    dateList.forEach(date => {
                        const drawdown = backtestData[date].equity.最大回撤;
                        if (drawdown < minDrawdown) {
                            minDrawdown = drawdown;
                            targetDate = date;
                        }
                    });
                    break;
            }
            
            if (targetDate) {
                currentDate = targetDate;
                document.getElementById('dateInput').value = targetDate;
                updateDisplay();
            }
            
            select.value = '';
        }
        
        // 上一天
        function previousDay() {
            const currentIndex = dateList.indexOf(currentDate);
            if (currentIndex > 0) {
                currentDate = dateList[currentIndex - 1];
                document.getElementById('dateInput').value = currentDate;
                updateDisplay();
            }
        }
        
        // 下一天
        function nextDay() {
            const currentIndex = dateList.indexOf(currentDate);
            if (currentIndex < dateList.length - 1) {
                currentDate = dateList[currentIndex + 1];
                document.getElementById('dateInput').value = currentDate;
                updateDisplay();
            }
        }
        
        // 播放动画
        function playAnimation() {
            const playBtn = document.getElementById('playBtn');
            
            if (isPlaying) {
                isPlaying = false;
                clearInterval(playInterval);
                playBtn.textContent = '▶️ 自动播放';
                playBtn.className = 'btn btn-primary';
                return;
            }
            
            isPlaying = true;
            playBtn.textContent = '⏸️ 暂停播放';
            playBtn.className = 'btn btn-secondary';
            
            const intervalTime = 1500 / playSpeed;
            
            playInterval = setInterval(() => {
                const currentIndex = dateList.indexOf(currentDate);
                if (currentIndex < dateList.length - 1) {
                    currentDate = dateList[currentIndex + 1];
                    document.getElementById('dateInput').value = currentDate;
                    updateDisplay();
                } else {
                    // 播放结束
                    isPlaying = false;
                    clearInterval(playInterval);
                    playBtn.textContent = '▶️ 自动播放';
                    playBtn.className = 'btn btn-primary';
                }
            }, intervalTime);
        }
        
        // 改变播放速度
        function changeSpeed() {
            const speedSelect = document.getElementById('speedSelect');
            playSpeed = parseFloat(speedSelect.value);
            
            // 如果正在播放，重新启动以应用新速度
            if (isPlaying) {
                clearInterval(playInterval);
                const intervalTime = 1500 / playSpeed;
                
                playInterval = setInterval(() => {
                    const currentIndex = dateList.indexOf(currentDate);
                    if (currentIndex < dateList.length - 1) {
                        currentDate = dateList[currentIndex + 1];
                        document.getElementById('dateInput').value = currentDate;
                        updateDisplay();
                    } else {
                        // 播放结束
                        isPlaying = false;
                        clearInterval(playInterval);
                        const playBtn = document.getElementById('playBtn');
                        playBtn.textContent = '▶️ 自动播放';
                        playBtn.className = 'btn btn-primary';
                    }
                }, intervalTime);
            }
        }
        
        // 更新心情模块
        function updateMood(dailyReturn, maxDrawdown) {
            const moodEmoji = document.getElementById('moodEmoji');
            const moodText = document.getElementById('moodText');
            
            let emoji, text, weatherType;
            
            // 计算回撤严重程度
            const drawdownLevel = Math.abs(maxDrawdown);
            
            // 优先根据回撤情况判断心情基调
            if (drawdownLevel > 0.25) {
                // 极度回撤：超过25%
                if (dailyReturn >= 0.03) {
                    emoji = '😤';
                    text = '虽然今天涨了，但回撤太深，心情复杂';
                    weatherType = 'cloudy';
                } else if (dailyReturn >= 0) {
                    emoji = '😰';
                    text = '回撤超过25%，压力巨大！';
                    weatherType = 'storm';
                } else {
                    emoji = '😭';
                    text = '深度回撤还在跌，心态崩了...';
                    weatherType = 'storm';
                }
            } else if (drawdownLevel > 0.15) {
                // 严重回撤：15%-25%
                if (dailyReturn >= 0.05) {
                    emoji = '😅';
                    text = '大涨了！但回撤还是让人担心';
                    weatherType = 'cloudy';
                } else if (dailyReturn >= 0.02) {
                    emoji = '😐';
                    text = '涨了点，但回撤不小，谨慎乐观';
                    weatherType = 'cloudy';
                } else if (dailyReturn >= 0) {
                    emoji = '😔';
                    text = '回撤较深，小涨也难掩忧虑';
                    weatherType = 'snowy';
                } else {
                    emoji = '😰';
                    text = '回撤不小还在跌，很焦虑';
                    weatherType = 'storm';
                }
            } else if (drawdownLevel > 0.08) {
                // 中等回撤：8%-15%
                if (dailyReturn >= 0.05) {
                    emoji = '😊';
                    text = '大涨！回撤在控制范围内';
                    weatherType = 'sunshine';
                } else if (dailyReturn >= 0.02) {
                    emoji = '😐';
                    text = '涨了点，回撤还算可控';
                    weatherType = 'cloudy';
                } else if (dailyReturn >= 0) {
                    emoji = '😕';
                    text = '小涨，但回撤让人不安';
                    weatherType = 'cloudy';
                } else if (dailyReturn >= -0.03) {
                    emoji = '😟';
                    text = '又跌了，回撤加深了';
                    weatherType = 'rainy';
                } else {
                    emoji = '😱';
                    text = '大跌！回撤雪上加霜';
                    weatherType = 'storm';
                }
            } else {
                // 轻微回撤：小于8%，主要看当日表现
                if (dailyReturn >= 0.08) {
                    emoji = '🚀';
                    text = '暴涨8%+！冲上云霄！';
                    weatherType = 'sunshine';
                } else if (dailyReturn >= 0.05) {
                    emoji = '🎉';
                    text = '大涨5%+！心情超棒！';
                    weatherType = 'sunshine';
                } else if (dailyReturn >= 0.03) {
                    emoji = '😄';
                    text = '涨了3%+，很开心！';
                    weatherType = 'sunshine';
                } else if (dailyReturn >= 0.01) {
                    emoji = '😊';
                    text = '小幅上涨，还不错';
                    weatherType = 'cloudy';
                } else if (dailyReturn >= -0.01) {
                    emoji = '😐';
                    text = '基本持平，波澜不惊';
                    weatherType = 'cloudy';
                } else if (dailyReturn >= -0.03) {
                    emoji = '😕';
                    text = '小幅下跌，有点不爽';
                    weatherType = 'rainy';
                } else if (dailyReturn >= -0.05) {
                    emoji = '😟';
                    text = '跌了3%+，有点担心';
                    weatherType = 'rainy';
                } else if (dailyReturn >= -0.08) {
                    emoji = '😱';
                    text = '大跌5%+，很难受';
                    weatherType = 'storm';
                } else {
                    emoji = '💀';
                    text = '暴跌8%+！心态爆炸！';
                    weatherType = 'storm';
                }
            }
            
            moodEmoji.textContent = emoji;
            moodText.textContent = text;
            
            // 更新天气效果
            updateWeather(weatherType);
        }
        
        // 更新天气效果
        function updateWeather(weatherType) {
            const weatherContainer = document.getElementById('weatherContainer');
            weatherContainer.innerHTML = ''; // 清除之前的天气效果
            
            // 移除所有背景类
            weatherContainer.className = 'weather-container';
            
            switch (weatherType) {
                case 'sunshine':
                    weatherContainer.classList.add('weather-bg-sunny');
                    createSunshine(weatherContainer);
                    break;
                case 'cloudy':
                    weatherContainer.classList.add('weather-bg-cloudy');
                    createClouds(weatherContainer);
                    break;
                case 'rainy':
                    weatherContainer.classList.add('weather-bg-rainy');
                    createRain(weatherContainer);
                    break;
                case 'snowy':
                    weatherContainer.classList.add('weather-bg-snowy');
                    createSnow(weatherContainer);
                    break;
                case 'storm':
                    weatherContainer.classList.add('weather-bg-stormy');
                    createStorm(weatherContainer);
                    break;
            }
        }
        
        // 创建阳光效果
        function createSunshine(container) {
            const sunshine = document.createElement('div');
            sunshine.className = 'sunshine';
            container.appendChild(sunshine);
            
            // 添加多个小太阳光点
            for(let i = 0; i < 8; i++) {
                setTimeout(() => {
                    const sparkle = document.createElement('div');
                    sparkle.style.position = 'absolute';
                    sparkle.style.width = '6px';
                    sparkle.style.height = '6px';
                    sparkle.style.background = '#FFD700';
                    sparkle.style.borderRadius = '50%';
                    sparkle.style.boxShadow = '0 0 10px #FFD700';
                    sparkle.style.top = Math.random() * 200 + 'px';
                    sparkle.style.right = Math.random() * 200 + 'px';
                    sparkle.style.animation = 'sparkle 2s ease-in-out infinite';
                    sparkle.style.animationDelay = Math.random() * 2 + 's';
                    container.appendChild(sparkle);
                    
                    setTimeout(() => {
                        if(sparkle.parentNode) sparkle.parentNode.removeChild(sparkle);
                    }, 4000);
                }, i * 200);
            }
        }
        
        // 创建云朵效果
        function createClouds(container) {
            // 创建大云朵
            for(let i = 0; i < 2; i++) {
                setTimeout(() => {
                    const cloud = document.createElement('div');
                    cloud.className = 'cloud cloud-large';
                    cloud.style.top = Math.random() * 150 + 'px';
                    cloud.style.animationDuration = (20 + Math.random() * 10) + 's';
                    cloud.style.animationDelay = Math.random() * 3 + 's';
                    container.appendChild(cloud);
                    
                    setTimeout(() => {
                        if(cloud.parentNode) cloud.parentNode.removeChild(cloud);
                    }, 35000);
                }, i * 3000);
            }
            
            // 创建小云朵
            for(let i = 0; i < 4; i++) {
                setTimeout(() => {
                    const cloud = document.createElement('div');
                    cloud.className = 'cloud cloud-small';
                    cloud.style.top = Math.random() * 250 + 'px';
                    cloud.style.animationDuration = (15 + Math.random() * 8) + 's';
                    cloud.style.animationDelay = Math.random() * 5 + 's';
                    container.appendChild(cloud);
                    
                    setTimeout(() => {
                        if(cloud.parentNode) cloud.parentNode.removeChild(cloud);
                    }, 30000);
                }, i * 1500);
            }
        }
        
        // 创建雨滴效果
        function createRain(container) {
            function addRainDrop() {
                const rain = document.createElement('div');
                const isHeavy = Math.random() < 0.3;
                rain.className = isHeavy ? 'rain rain-heavy' : 'rain';
                rain.style.left = Math.random() * 100 + '%';
                rain.style.animationDuration = (0.4 + Math.random() * 0.4) + 's';
                rain.style.animationDelay = Math.random() * 0.1 + 's';
                container.appendChild(rain);
                
                setTimeout(() => {
                    if(rain.parentNode) rain.parentNode.removeChild(rain);
                }, 1200);
            }
            
            // 持续创建雨滴
            const rainInterval = setInterval(() => {
                if(container.children.length < 80) {
                    addRainDrop();
                    // 偶尔添加额外的雨滴
                    if(Math.random() < 0.3) {
                        setTimeout(addRainDrop, 50);
                    }
                }
            }, 60);
            
            setTimeout(() => {
                clearInterval(rainInterval);
            }, 12000);
        }
        
        // 创建雪花效果
        function createSnow(container) {
            const snowflakes = ['❄', '❅', '❆', '✻', '✼', '❋', '❉', '❈'];
            
            function addSnowflake() {
                const snow = document.createElement('div');
                const isLarge = Math.random() < 0.2;
                snow.className = isLarge ? 'snow snow-large' : 'snow';
                snow.textContent = snowflakes[Math.floor(Math.random() * snowflakes.length)];
                snow.style.left = Math.random() * 100 + '%';
                snow.style.animationDuration = (4 + Math.random() * 4) + 's';
                snow.style.animationDelay = Math.random() * 2 + 's';
                snow.style.opacity = 0.7 + Math.random() * 0.3;
                container.appendChild(snow);
                
                setTimeout(() => {
                    if(snow.parentNode) snow.parentNode.removeChild(snow);
                }, 10000);
            }
            
            // 持续创建雪花
            const snowInterval = setInterval(() => {
                if(container.children.length < 50) {
                    addSnowflake();
                    // 偶尔添加额外的大雪花
                    if(Math.random() < 0.15) {
                        setTimeout(() => {
                            const extraSnow = document.createElement('div');
                            extraSnow.className = 'snow snow-large';
                            extraSnow.textContent = snowflakes[Math.floor(Math.random() * snowflakes.length)];
                            extraSnow.style.left = Math.random() * 100 + '%';
                            extraSnow.style.animationDuration = (5 + Math.random() * 3) + 's';
                            extraSnow.style.opacity = 0.9;
                            container.appendChild(extraSnow);
                            
                            setTimeout(() => {
                                if(extraSnow.parentNode) extraSnow.parentNode.removeChild(extraSnow);
                            }, 10000);
                        }, 100);
                    }
                }
            }, 200);
            
            setTimeout(() => {
                clearInterval(snowInterval);
            }, 18000);
        }
        
        // 创建暴风雨效果
        function createStorm(container) {
            // 创建强雨滴
            function addStormRain() {
                const rain = document.createElement('div');
                rain.className = 'rain rain-heavy';
                rain.style.left = Math.random() * 100 + '%';
                rain.style.animationDuration = (0.2 + Math.random() * 0.3) + 's';
                rain.style.animationDelay = Math.random() * 0.05 + 's';
                container.appendChild(rain);
                
                setTimeout(() => {
                    if(rain.parentNode) rain.parentNode.removeChild(rain);
                }, 800);
            }
            
            // 持续创建暴雨
            const stormRainInterval = setInterval(() => {
                if(container.children.length < 120) {
                    addStormRain();
                    addStormRain(); // 双倍雨滴
                    if(Math.random() < 0.5) {
                        setTimeout(addStormRain, 20);
                    }
                }
            }, 30);
            
            // 创建主闪电
            function addMainLightning() {
                const lightning = document.createElement('div');
                lightning.className = 'lightning';
                lightning.style.left = (40 + Math.random() * 20) + '%';
                container.appendChild(lightning);
                
                // 添加分支闪电
                setTimeout(() => {
                    for(let i = 0; i < 3; i++) {
                        const branch = document.createElement('div');
                        branch.className = 'lightning-branch';
                        branch.style.left = (35 + Math.random() * 30) + '%';
                        branch.style.top = (40 + Math.random() * 30) + '%';
                        branch.style.transform = 'rotate(' + (Math.random() * 60 - 30) + 'deg)';
                        container.appendChild(branch);
                        
                        setTimeout(() => {
                            if(branch.parentNode) branch.parentNode.removeChild(branch);
                        }, 400);
                    }
                }, 50);
                
                setTimeout(() => {
                    if(lightning.parentNode) lightning.parentNode.removeChild(lightning);
                }, 400);
            }
            
            // 随机强闪电
            const lightningInterval = setInterval(() => {
                if(Math.random() < 0.4) {
                    addMainLightning();
                    // 偶尔连续闪电
                    if(Math.random() < 0.3) {
                        setTimeout(addMainLightning, 200);
                    }
                }
            }, 800);
            
            setTimeout(() => {
                clearInterval(stormRainInterval);
                clearInterval(lightningInterval);
            }, 15000);
        }
        
        // 页面加载完成后初始化
        document.addEventListener('DOMContentLoaded', function() {
            initData();
        });
    </script>
</body>
</html>
        """
    
    def generate_report(self, output_file: str = None) -> str:
        """
        生成HTML报告
        
        Args:
            output_file: 输出文件路径，如果为None则自动生成
            
        Returns:
            str: 生成的HTML文件路径
        """
        if not self.load_data():
            raise Exception("无法加载数据")
        
        # 处理数据
        daily_data = self.process_daily_data()
        
        # 生成HTML
        html_template = self.generate_html_template()
        html_content = html_template.replace('{BACKTEST_DATA}', json.dumps(daily_data, ensure_ascii=False, indent=2))
        
        # 确定输出文件路径
        if output_file is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = os.path.join(self.data_dir, f'回测报告_{timestamp}.html')
        
        # 写入文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML报告已生成: {output_file}")
        return output_file


def main():
    """
    主函数 - 命令行使用示例
    """
    import sys


    # 从命令行参数获取数据目录
    data_dir = sys.argv[1] if len(sys.argv) > 1 else default_data_dir
    
    try:
        # 创建报告生成器
        generator = BacktestReportGenerator(data_dir)
        
        # 生成报告
        output_file = generator.generate_report()
        
        print(f"\n✅ 报告生成成功!")
        print(f"📁 文件位置: {output_file}")
        print(f"🌐 请用浏览器打开查看报告")
        
        # 尝试自动打开浏览器
        try:
            print(f"尝试自动打开浏览器")
            # 这个是chrome
            # import webbrowser
            # webbrowser.get('edge').open(f'file:///{output_file.replace(chr(92), "/")}')

            # 这个是edge
            import subprocess
            import os
            subprocess.Popen(['open', '-a', 'Microsoft Edge', f'file:///{output_file.replace(chr(92), "/")}'])

            print(f"🚀 已尝试自动打开浏览器")

        except:
            print(f"尝试自动打开浏览器报错")
            pass
            
    except Exception as e:
        print(f"❌ 生成报告时出错: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()