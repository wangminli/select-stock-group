"""
邢不行™️选股框架
Python股票量化投资课程

版权所有 ©️ 邢不行
微信: xbx8662

未经授权，不得复制、修改、或使用本代码的全部或部分内容。仅限个人学习用途，禁止商业用途。

Author: 邢不行
"""
import importlib


def get_strategy_by_name(name) -> dict:
    try:
        # 构造模块名
        module_name = f"策略库.{name}"

        # 动态导入模块
        strategy_module = importlib.import_module(module_name)

        # 创建一个包含模块变量和函数的字典
        strategy_content = {
            name: getattr(strategy_module, name)
            for name in dir(strategy_module)
            if not name.startswith("__") and callable(getattr(strategy_module, name))
        }

        return strategy_content
    except ModuleNotFoundError:
        return {}
        # raise ValueError(f"Strategy {strategy_name} not found.")
    except AttributeError:
        raise ValueError(f"Error accessing strategy content in module {name}.")
