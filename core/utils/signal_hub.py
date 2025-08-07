"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""

import importlib


def get_signal_by_name(name):
    try:
        # 构造模块名
        module_name = f"信号库.{name}"

        # 动态导入模块
        signal_module = importlib.import_module(module_name)

        # 创建一个包含模块变量和函数的字典
        signal_content = {
            attr_name: getattr(signal_module, attr_name)
            for attr_name in dir(signal_module)
            if not attr_name.startswith("__") and callable(getattr(signal_module, attr_name))
        }
        return signal_content
    except ModuleNotFoundError:
        raise ValueError(f"Signal {name} not found.")
    except AttributeError:
        raise ValueError(f"Error accessing signal content in module {name}.")
