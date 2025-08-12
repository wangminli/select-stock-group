# -*- coding: utf-8 -*-
"""
邢不行｜策略分享会
选股策略框架𝓟𝓻𝓸

版权所有 ©️ 邢不行
微信: xbx1717

本代码仅供个人学习使用，未经授权不得复制、修改或用于商业用途。

Author: 邢不行
"""

import itertools
import operator
import os
import traceback
import warnings
from functools import reduce
from pathlib import Path
import pandas as pd

import tools.utils.pfunctions as pf
from core.utils.path_kit import get_folder_path

warnings.filterwarnings("ignore")


# ====== 公共函数 ======
def dict_itertools(dict_):
    keys = list(dict_.keys())
    values = list(dict_.values())
    return [dict(zip(keys, combo)) for combo in itertools.product(*values)]


def filter_dataframe(df, filter_dict):
    conditions = [df[col].isin(values) for col, values in filter_dict.items()]
    return df[reduce(operator.and_, conditions)] if conditions else df.copy()


def prepare_data():
    """生成参数组合并过滤"""
    params_df = pd.DataFrame(dict_itertools(batch))
    try:
        params_df["参数组合"] = sorted(
            [x for x in result_folder_path.iterdir() if x.is_dir()], key=lambda x: int(x.stem.split("_")[1])
        )
    except ValueError:
        traceback.print_exc()
        print("请检查trav_name是否有误")
        exit()
    return filter_dataframe(params_df, limit_dict)


def load_and_process_data(df_left, result_dir: Path):
    """加载并处理策略评价数据"""
    if evaluation_indicator not in [
        "累积净值",
        "年化收益",
        "最大回撤",
        "年化收益/回撤比",
        "盈利周期数",
        "亏损周期数",
        "胜率",
        "每周期平均收益",
        "盈亏收益比",
        "单周期最大盈利",
        "单周期大亏损",
        "最大连续盈利周期数",
        "最大连续亏损周期数",
        "收益率标准差",
    ]:
        raise ValueError("评价指标有误，按要求输入")

    if evaluation_indicator == "年化收益":
        time_list = []
        for folder in df_left["参数组合"]:
            # 读取策略评价数据
            stats_path = result_dir / folder / ("策略评价_再择时.csv" if "re_timing" in batch else "策略评价.csv")
            stats_temp = pd.read_csv(stats_path, encoding="utf-8")
            stats_temp.columns = ["evaluation_indicator", "value"]
            if stats_temp.empty:
                raise ValueError(f"{folder} 文件夹内策略评价数据为空，请检查数据")
            stats_temp = stats_temp.set_index("evaluation_indicator")
            df_left.loc[df_left["参数组合"] == folder, "all"] = stats_temp.loc[evaluation_indicator, "value"]

            # 读取年度数据
            years_path = result_dir / folder / ("年度账户收益_再择时.csv" if "re_timing" in batch else "年度账户收益.csv")
            years_return = pd.read_csv(years_path, encoding="utf-8")
            if years_return.empty:
                raise ValueError(f"{folder} 文件夹内年度账户收益数据为空，请检查数据")
            time_list = list(years_return["交易日期"].sort_values(ascending=False))
            for time in time_list:
                df_left.loc[df_left["参数组合"] == folder, time] = years_return.loc[
                    years_return["交易日期"] == time, "涨跌幅"
                ].iloc[0]

        # 格式转换
        df_left[["all"] + time_list] = df_left[["all"] + time_list].map(
            lambda x: float(x.replace("%", "")) / 100 if "%" in str(x) else float(x)
        )
        return time_list
    else:
        for folder in df_left["参数组合"]:
            stats_path = result_dir / folder / ("策略评价_再择时.csv" if "re_timing" in batch else "策略评价.csv")
            stats_temp = pd.read_csv(stats_path, encoding="utf-8")
            if stats_temp.empty:
                raise ValueError(f"{folder} 文件夹内策略评价数据为空，请检查数据")
            stats_temp.columns = ["evaluation_indicator", "value"]
            stats_temp = stats_temp.set_index("evaluation_indicator")
            df_left.loc[df_left["参数组合"] == folder, evaluation_indicator] = stats_temp.loc[
                evaluation_indicator, "value"
            ]

        df_left[evaluation_indicator] = df_left[evaluation_indicator].apply(
            lambda x: float(x.replace("%", "")) / 100 if "%" in str(x) else float(x)
        )
        return None


def generate_plots(df_left, params, output_dir: Path, analysis_type, time_list):
    """根据分析类型生成图表"""
    fig_list = []
    html_name = f"年化收益_回撤比.html" if evaluation_indicator == "年化收益/回撤比" else f"{evaluation_indicator}.html"

    if "hold_period" in df_left.columns:
        df_left["periods"] = df_left["hold_period"].apply(lambda x: int(x[:-1]))
        df_left = df_left.sort_values(by=["periods"])

    if analysis_type == "double":
        x_, y_ = params

        if evaluation_indicator == "年化收益":
            for time in ["all"] + time_list:
                temp = pd.pivot_table(df_left, index=y_, columns=x_, values=time)
                fig = pf.draw_params_heatmap_plotly(temp, title=time)
                fig_list.append(fig)
        else:
            temp = pd.pivot_table(df_left, index=y_, columns=x_, values=evaluation_indicator)
            fig = pf.draw_params_heatmap_plotly(temp, title=evaluation_indicator)
            fig_list.append(fig)
        html_name = f"{x_}_{y_}_{html_name}"

    else:
        param = params
        if evaluation_indicator == "年化收益":
            sub_df = df_left[[param] + ["all"] + time_list].copy()
            sub_df[param] = sub_df[param].map(lambda x: f"{param}_{x}")
            sub_df = sub_df.set_index(param)
            fig = pf.draw_params_bar_plotly(sub_df, evaluation_indicator)
        else:
            x_axis = df_left[param].map(lambda x: f"{param}_{x}")
            fig = pf.draw_bar_plotly(
                x_axis, df_left[evaluation_indicator], title=evaluation_indicator, pic_size=[1800, 600]
            )
        fig_list.append(fig)
        html_name = f"{param}_{html_name}"

    if fig_list:
        title = "参数热力图" if analysis_type == "double" else "参数平原图"
        pf.merge_html_flexible(fig_list, output_dir / html_name, title=title)


# ====== 主逻辑 ======
def analyze_params(analysis_type):
    """参数分析主函数"""
    df_left = prepare_data()

    # 配置输出路径
    if analysis_type == "double":
        output_dir = out_folder_path / "参数热力图" / trav_name
        params = [param_x, param_y]
    else:
        output_dir = out_folder_path / "参数平原图" / trav_name
        params = param_x
    os.makedirs(output_dir, exist_ok=True)

    # 处理数据
    time_list = load_and_process_data(df_left, result_folder_path)

    # 生成图表
    generate_plots(df_left, params, output_dir, analysis_type, time_list)


if __name__ == "__main__":
    # 仅修改 __main__：将单次回测结果包装为一次“遍历结果”以便现有逻辑直接运行
    from shutil import copy2, rmtree

    # ====== 配置信息 ======
    trav_name = "市值+资金流强度"

    # 参数分析输出路径
    out_folder_path = get_folder_path("data", "分析结果", "参数分析")

    # 参数设置：按 batch 的组合数自动创建 参数组合_1..N
    batch = {
        "select_num": [1, 3, 5],
        # 注意，re_timing如果没用的话，一定要注释掉，不然会导致结果出现误差
      "re_timing": [100,200,300],
    }

    # 源数据：单次回测结果目录
    src_single_result_dir = get_folder_path("data", "回测结果", trav_name, auto_create=False)

    # 目标数据：临时遍历结果目录（与 prepare_data 期望一致）
    result_folder_path = get_folder_path("data", "遍历结果", trav_name, auto_create=True)

    # 清理旧的 参数组合_* 目录，避免数量不一致
    for sub in result_folder_path.iterdir():
        if sub.is_dir() and sub.name.startswith("参数组合_"):
            try:
                rmtree(sub)
            except Exception:
                pass

    # 根据 batch 的组合数量复制数据
    combo_count = len(dict_itertools(batch))
    for i in range(1, combo_count + 1):
        temp_combo_dir = result_folder_path / f"参数组合_{i}"
        os.makedirs(temp_combo_dir, exist_ok=True)
        for item in src_single_result_dir.iterdir():
            if item.is_file():
                copy2(item, temp_combo_dir / item.name)

    # 单参数平原图；如需热力图可为 param_y 赋值为 "re_timing" 并拓展 batch 与 目录
    param_x = "select_num"
    param_y = "re_timing"

    limit_dict = {}

    # 分析指标
    evaluation_indicator = "累积净值"

    # ====== 主逻辑 ======
    analysis_type = "single" if len(param_y.strip()) == 0 else "double"
    analyze_params(analysis_type)
