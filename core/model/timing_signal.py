"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

本代码仅供个人学习使用，未经授权不得复制、修改或用于商业用途。

Author: 邢不行
"""

from dataclasses import dataclass, field
from typing import Callable, Dict
import pandas as pd

from core.model.strategy_config import parse_param
from core.utils.signal_hub import get_signal_by_name


@dataclass
class EquityTiming:
    name: str
    params: list | tuple = ()

    # 策略函数
    funcs: Dict[str, Callable] = field(default_factory=dict)

    @classmethod
    def init(cls, **config) -> "EquityTiming":
        config["params"] = parse_param(config.get("params", ()))
        config["funcs"] = get_signal_by_name(config["name"])
        leverage_signal = cls(**config)

        return leverage_signal

    def get_equity_signal(self, equity_df: pd.DataFrame) -> pd.Series:
        return self.funcs["equity_signal"](equity_df, *self.params)
