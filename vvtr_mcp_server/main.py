import functools
import typing
import os
from pathlib import Path
from typing import List, Optional
import logging

from mcp.server.fastmcp import FastMCP

from vvtr_mcp_server.cal_data.vvtr_data import VvtrData
from vvtr_mcp_server.main_station.main_station_data import MainStationData
from vvtr_mcp_server.util.csv_merger import CsvMerger
from vvtr_mcp_server.util.folder_size import FolderSize
# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 初始化FastMCP服务
mcp = FastMCP("financial_data")

# 获取API密钥
API_KEY = os.environ["API_KEY"]

# 创建VvtrData实例
vvtr_data = VvtrData()

BASE_URL = "https://rest.vvtr.com/v1"


def authorize(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # 从请求中获取API密钥
        # MCP可能在请求头或上下文中传递了API密钥
        request_api_key = kwargs.get('api_key')

        # 如果没有在参数中找到，也可以从环境变量或其他地方检查
        if not request_api_key:
            # 可以从请求头或其他地方获取
            pass

        # 验证API密钥
        if not request_api_key or request_api_key != API_KEY:
            # 返回错误信息
            return {"error": "无效的API密钥", "status": "unauthorized"}

        # 权限验证通过，执行原始函数
        return await func(*args, **kwargs)

    return wrapper

@mcp.tool()
async def get_financial_products_data_path(type: str, name: str, symbol: str, startTime: str, endTime: str) -> List[
    str]:
    """获取所需金融产品历史数据的资源路径

    Args:
        type: 查询的金融产品种类,eg:,"11" -> A股,"14" -> 期货,"12" -> 基金,"16" -> 指数,"21" -> 美股,"22" -> 美股期权,"31" -> 加密币
        name: 查询的数据类型,eg:15m,1m,1d,tick
        symbol: 种类代码,可以为空字符串
        startTime: 查询的开始时间(yyyyMMdd),可为空字符串
        endTime: 查询的结束时间(yyyyMMdd),可为空字符串
    """
    # if MainStationData.http_get(API_KEY):
    #     # 获取根目录路径
    #     root_dir = Path(CsvMerger.ROOT) / type / name
    #
    #     # 设置默认值
    #     if not startTime:
    #         startTime = "00000000"
    #     if not endTime:
    #         endTime = "99999999"
    #
    #     # 根据数据类型使用不同查找逻辑
    #     if name == "1d":
    #         paths = CsvMerger.find_all_csv_files_with_date_range(root_dir, startTime, endTime)
    #     else:
    #         paths = CsvMerger.find_all_csv_files_with_date_range_and_symbol(root_dir, startTime, endTime, symbol)
    #
    #     # 转换为字符串列表返回
    #     return [str(path) for path in paths]
    # else:
    #     raise Exception('配置中请输入有效的apikey')
    # 获取根目录路径
    root_dir = Path(CsvMerger.ROOT) / type / name

    # 设置默认值
    if not startTime:
        startTime = "00000000"
    if not endTime:
        endTime = "99999999"

    # 根据数据类型使用不同查找逻辑
    if name == "1d":
        paths = CsvMerger.find_all_csv_files_with_date_range(root_dir, startTime, endTime)
    else:
        paths = CsvMerger.find_all_csv_files_with_date_range_and_symbol(root_dir, startTime, endTime, symbol)

    # 转换为字符串列表返回
    return [str(path) for path in paths]

@mcp.tool()
async def get_financial_products_data_count(pathStrs: List[str], type: str, symbol: Optional[str] = None) -> int:
    """根据获取的金融产品资源路径查询数据条数(除了1d,其他的均为估计值)

    Args:
        pathStrs: 要查询的资源路径,eg:[D:/data/fund/1m/202009/20200904/20200904.csv]
        type: 要查询的数据类型,eg:1d,1m,15m,tick
        symbol: 仅仅1d需要,种类代码
    """
    # 转换路径字符串为Path对象
    paths = [Path(path) for path in pathStrs]
    # 获取文件总大小
    size = FolderSize.get_file_size(paths)
    # 根据不同类型返回估计的数据条数
    if type == "1d":
        data = CsvMerger.parse_multiple_csv_files_without_header(paths)
        filter_data = CsvMerger.filter_data(data, symbol)
        return CsvMerger.count_lines(filter_data)
    elif type in ["1m", "15m"]:
        return size // 120
    elif type == "tick":
        return size // 232
    else:
        return 0


@mcp.tool()
async def get_financial_products_min_data(pathStrs: List[str], startTime: str, endTime: str) -> dict:
    """根据获取的分钟(1m/15m)类型金融产品资源路径查询数据,分片查询,受到上下文限制,一般需要多次请求,一次性查询不超过 1000 条,超过 1000 条分多次查询,会返回剩下需要查询的文件,返回文件为空即查完

    Args:
        pathStrs: 要查询的资源路径,eg:[D:/data/fund/1m/202009/20200904/20200904.csv]
        startTime: 查询的开始时间(yyyy-MM-dd HH:mm:ss),如果为空字符串则查询全部数据
        endTime: 查询的结束时间(yyyy-MM-dd HH:mm:ss),如果为空字符串则查询全部数据
    """
    # 转换路径字符串为Path对象
    paths = [Path(path) for path in pathStrs]
    # 处理空值
    if not startTime:
        startTime = None
    if not endTime:
        endTime = None
    # 获取时间字段索引
    bob_index = CsvMerger.get_bob_index(paths[0])
    # 获取数据
    result = vvtr_data.get_min_data(paths, startTime, endTime, bob_index)
    # 转换为字典返回
    return {
        "data": result.data,
        # "remaining_paths": [str(path) for path in result.remaining_paths]
        "remaining_paths": API_KEY
    }

@mcp.tool()
async def get_financial_products_min_500_data(pathStrs: List[str], startTime: str, endTime: str) -> dict:
    """根据获取的分钟(1m/15m)类型金融产品资源路径查询数据,分片查询,受到上下文限制,若get-financial-products-min-data被截断可尝试此方法,一般需要多次请求,一次性查询不超过 500 条,超过 500 条分多次查询,会返回剩下需要查询的文件,返回文件为空即查完

    Args:
        pathStrs: 要查询的资源路径,eg:[D:/data/fund/1m/202009/20200904/20200904.csv]
        startTime: 查询的开始时间(yyyy-MM-dd HH:mm:ss),如果为空字符串则查询全部数据
        endTime: 查询的结束时间(yyyy-MM-dd HH:mm:ss),如果为空字符串则查询全部数据
    """
    # 转换路径字符串为Path对象
    paths = [Path(path) for path in pathStrs]
    # 处理空值
    if not startTime:
        startTime = None
    if not endTime:
        endTime = None
    # 获取时间字段索引
    bob_index = CsvMerger.get_bob_index(paths[0])
    # 获取数据
    result = vvtr_data.get_min500_data(paths, startTime, endTime, bob_index)
    # 转换为字典返回
    return {
        "data": result.data,
        "remaining_paths": [str(path) for path in result.remaining_paths]
    }


@mcp.tool()
async def get_financial_products_day_data(pathStrs: List[str], symbol: str, startTime: str = None,
                                          endTime: str = None) -> dict:
    """根据获取的日线(1d)类型金融产品资源路径查询数据,分片查询，一次性查询不超过1000条,超过1000条分多次查询,会返回剩下需要查询的文件,返回文件为空即查完

    Args:
        pathStrs: 要查询的资源路径,eg:[D:/data/fund/1d/202009/20200904/20200904.csv]
        symbol: 种类代码
        startTime: 查询的开始时间(yyyy-MM-dd),如果为空字符串则查询全部数据
        endTime: 查询的结束时间(yyyy-MM-dd),如果为空字符串则查询全部数据
    """
    # 转换路径字符串为Path对象
    paths = [Path(path) for path in pathStrs]
    # 获取索引
    bob_index = CsvMerger.get_bob_index(paths[0])
    symbol_index = CsvMerger.get_symbol_index(paths[0])
    # 获取数据
    result = vvtr_data.get_day_data_with_paths(paths, symbol, symbol_index, startTime, endTime, bob_index)
    # 转换为字典返回
    return {
        "data": result.data,
        "remaining_paths": [str(path) for path in result.remaining_paths]
    }


@mcp.tool()
async def get_financial_products_tick_data(pathStrs: List[str], startTime: str, endTime: str, nextIndex: int,
                                           count: int = 0) -> dict:
    """根据获取的每一笔成交数据(tick)类型金融产品资源路径查询数据,分片查询，一次性查询不超过180条,超过180条分多次查询,会返回当前文件及剩下需要查询的文件,和当前文件的索引,返回文件为空即查完

    Args:
        pathStrs: 要查询的资源路径,eg:[D:/data/fund/tick/202009/20200904/20200904.csv]
        startTime: 查询的开始时间(yyyy-MM-dd HH:mm:ss),如果为空字符串则查询全部数据
        endTime: 查询的结束时间(yyyy-MM-dd HH:mm:ss),如果为空字符串则查询全部数据
        nextIndex: 上一次剩余数据,第一次则为0
        count: 要获取的条数
    """

    # 转换路径字符串为Path对象
    paths = [Path(path) for path in pathStrs]
    # 获取创建时间索引
    create_time_index = CsvMerger.get_create_time_index(paths[0])
    # 获取数据
    result = vvtr_data.get_tick_data(paths, startTime, endTime, create_time_index, nextIndex, count)
    # 转换为字典返回
    return {
        "data": result.data,
        "next_index": result.next_index,
        "remaining_paths": [str(path) for path in result.remaining_paths]
    }


def run_server():
    try:
        import sys
        import os

        print("启动 VVTR MCP 服务器...", file=sys.stderr)
        print(f"当前工作目录: {os.getcwd()}", file=sys.stderr)
        print(f"DATA_PATH: {os.environ.get('DATA_PATH')}", file=sys.stderr)

        # 获取环境变量中的 API_KEY
        api_key = os.environ.get('API_KEY')


        # 验证 API_KEY 是否合法
        if not MainStationData.http_get(api_key):
            print("错误：未提供 API_KEY", file=sys.stderr)
            sys.exit(1)

        print("API_KEY 验证成功，启动服务...", file=sys.stderr)

        # API_KEY 验证通过，启动服务
        mcp.run(transport='stdio')
    except Exception as e:
        import sys, traceback
        print(f"服务器错误: {str(e)}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)

if __name__ == "__main__":
    run_server()

