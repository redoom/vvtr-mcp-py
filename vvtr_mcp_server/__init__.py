"""VVTR MCP服务器 - 金融数据查询工具"""

__version__ = "0.1.0"

# 导入主要功能
from .main import run_server

# 暴露主要函数
__all__ = ["run_server"]