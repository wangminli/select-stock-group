"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""
import os

import plotly.graph_objects as go
from plotly.offline import plot
from plotly.subplots import make_subplots

from core.utils.path_kit import get_file_path


def draw_equity_curve_plotly(df, data_dict, date_col=None, right_axis=None, pic_size=None, chg=False,
                             title=None, path=get_file_path('data', 'pic.html'), show=True, desc=None, strategy_metrics=None):
    """
    绘制策略曲线
    :param df: 包含净值数据的df
    :param data_dict: 要展示的数据字典格式：｛图片上显示的名字:df中的列名｝
    :param date_col: 时间列的名字，如果为None将用索引作为时间列
    :param right_axis: 右轴数据 ｛图片上显示的名字:df中的列名｝
    :param pic_size: 图片的尺寸
    :param chg: datadict中的数据是否为涨跌幅，True表示涨跌幅，False表示净值
    :param title: 标题
    :param path: 图片路径
    :param show: 是否打开图片
    :param desc: 图表描述
    :param strategy_metrics: 策略评价指标字典
    :return:
    """
    if pic_size is None:
        pic_size = [1500, 800]

    draw_df = df.copy()

    # 设置时间序列
    if date_col:
        time_data = draw_df[date_col]
    else:
        time_data = draw_df.index

    # 绘制左轴数据
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    for key in data_dict:
        if chg:
            draw_df[data_dict[key]] = (draw_df[data_dict[key]] + 1).fillna(1).cumprod()
        fig.add_trace(go.Scatter(x=time_data, y=draw_df[data_dict[key]], name=key, ))

    # 绘制右轴数据
    if right_axis:
        key = list(right_axis.keys())[0]
        fig.add_trace(go.Scatter(x=time_data, y=draw_df[right_axis[key]], name=key + '(右轴)',
                                 marker=dict(color='rgba(220, 220, 220, 0.8)'),
                                 # marker_color='orange',
                                 opacity=0.1, line=dict(width=0),
                                 fill='tozeroy',
                                 yaxis='y2'))  # 标明设置一个不同于trace1的一个坐标轴
        for key in list(right_axis.keys())[1:]:
            fig.add_trace(go.Scatter(x=time_data, y=draw_df[right_axis[key]], name=key + '(右轴)',
                                     #  marker=dict(color='rgba(220, 220, 220, 0.8)'),
                                     opacity=0.1, line=dict(width=0),
                                     fill='tozeroy',
                                     yaxis='y2'))  # 标明设置一个不同于trace1的一个坐标轴

    # 如果有策略评价指标，添加表格
    if strategy_metrics:
        # 创建策略评价指标表格
        table_trace = go.Table(
            header=dict(
                values=['指标名称', '数值'],
                font=dict(size=12, color='white'),
                fill_color='rgb(49, 130, 189)',
                align='center'
            ),
            cells=dict(
                values=[
                    list(strategy_metrics.keys()),
                    list(strategy_metrics.values())
                ],
                font=dict(size=11, color='black'),
                fill_color='rgb(245, 245, 245)',
                align='center',
                height=25
            ),
            domain=dict(x=[0.80, 1.0], y=[0.02, 0.82])
        )
        fig.add_trace(table_trace)

    fig.update_layout(template="none", width=pic_size[0], height=pic_size[1], title_text=title,
                      hovermode="x unified", hoverlabel=dict(bgcolor='rgba(255,255,255,0.5)', ),
                      annotations=[
                          dict(
                              text=desc,
                              xref='paper',
                              yref='paper',
                              x=0.5,
                              y=1.06,
                              showarrow=False,
                              font=dict(size=12, color='black'),
                              align='center',
                              bgcolor='rgba(255,255,255,0.8)',
                          )
                      ]
                      )
    # 统一设置悬浮框中的日期格式为 yyyy-MM-dd
    fig.update_xaxes(hoverformat='%Y-%m-%d')
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=[
                    dict(label="线性 y轴",
                         method="relayout",
                         args=[{"yaxis.type": "linear"}]),
                    dict(label="Log y轴",
                         method="relayout",
                         args=[{"yaxis.type": "log"}]),
                ])],
    )
    
    # 设置X轴范围，为表格留出空间
    fig.update_xaxes(domain=[0.0, 0.73])
    
    # 设置图例位置
    fig.update_layout(
        showlegend=True,
        legend=dict(
            x=0.8,
            y=1.0,
            bgcolor='white',
            bordercolor='gray',
            borderwidth=1
        )
    )
    
    plot(figure_or_data=fig, filename=str(path), auto_open=False)

    fig.update_yaxes(
        showspikes=True, spikemode='across', spikesnap='cursor', spikedash='solid', spikethickness=1,  # 峰线
    )
    fig.update_xaxes(
        showspikes=True, spikemode='across+marker', spikesnap='cursor', spikedash='solid', spikethickness=1,  # 峰线
    )

    # 打开图片的html文件，需要判断系统的类型
    if show:
        res = os.system('start ' + str(path))
        if res != 0:
            os.system('open ' + str(path))
