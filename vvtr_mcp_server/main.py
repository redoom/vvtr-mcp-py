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
API_KEY = os.environ.get("API_KEY", '123456')

# 创建VvtrData实例
vvtr_data = VvtrData()

BASE_URL = "https://rest.vvtr.com/v1"



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

@mcp.tool()
async def get_symbol_count(type: str) -> int:
    """查询当日各品种下活跃的symbol的数量。

    Args:
        type: 要查询的资源路径,eg:"11" -> A股, "14" -> 期货, "12" -> 基金, "16" -> 指数, "21" -> 美股, "22" -> 美股期权, "31" -> 加密币
    """
    data = MainStationData.get_symbol(type, API_KEY)
    return MainStationData.count_lines_in_string(data)


@mcp.tool()
async def get_online_symbol(type: str, start: int, end: int):
    """查询当日各品种下活跃的symbol，每日盘前更新，需要先统计一下数量,建议一次性获取1000条。

    Args:
        type: 要查询的资源路径,eg:"11" -> A股, "14" -> 期货, "12" -> 基金, "16" -> 指数, "21" -> 美股, "22" -> 美股期权, "31" -> 加密币
        start: 查询的起始条数
        end: 查询的结束条数
    """
    data = MainStationData.get_symbol(type, API_KEY)
    res = MainStationData.cut_data(data, start, end)
    return res

@mcp.tool()
async def get_online_history_kline(symbols: str, interval: str, type: str,
                          from_date: str, to_date: str, adjust: bool = False,
                          limit: int = 2000, cursor_token: str = None) -> tuple[str | None, bool, str]:
    """
            获取在线数据的历史K线数据

            Args:
                symbols: 证券代码,多个代码用逗号分隔,填"*"则提交分类下的全部symbol
                interval: 周期(如1m, 5m, 1d等)
                type: 产品类型,eg:"11" -> A股, "14" -> 期货, "12" -> 基金, "16" -> 指数, "21" -> 美股, "22" -> 美股期权, "31" -> 加密币
                from_date: 开始时间，若查询24H内K线时间格式用 yyyy-mm-dd HH:mm:ss
                to_date: 结束时间，与from格式保持一致
                adjust: 是否复权
                limit: 单次返回的最大数量(默认2000)
                cursor_token: 分页游标标记。当接口返回的响应中包含此字段时，表示当前数据未完全加载，
                             需要将此值作为参数传入下一次请求以获取下一页数据。

            Returns:
                返回元组(csv_data, has_next, next_cursor_token)：
                - csv_data: CSV格式的K线数据或None(如果请求失败)
                - has_next: 布尔值，表示是否还有更多数据未返回
                - next_cursor_token: 字符串，下一页的游标标记，如果没有更多数据则为空字符串''
            """
    return MainStationData.get_history_kline(symbols, interval, type, API_KEY, from_date, to_date, adjust, limit, cursor_token)

@mcp.tool()
async def get_online_current_kline(type: str, symbols: str = None) -> str | None:
    """
    在线获取最新分钟K线数据

    Args:
        type: 产品类型，可填写多个，用","分隔。各个type的权限需独立获取。
        apikey: 您的apiKey
        symbols: 证券代码，用","分隔，多个type的symbol用";"分隔，顺序务必与type保持一致。

    Returns:
        返回CSV格式的最新分钟K线数据或None(如果请求失败)
    """
    return MainStationData.get_current_kline(type, API_KEY, symbols)

@mcp.tool()
async def get_online_latest_tick(type: str, symbols: str = None) -> str | None:
    """
    获取最新tick数据

    Args:
        type: 产品类型，可填写多个，用","分隔。各个type的权限需独立获取。
        apikey: 您的apiKey
        symbols: 证券代码，用","分隔，多个type的symbol用";"分隔，顺序务必与type保持一致。

    Returns:
        返回CSV格式的最新tick数据或None(如果请求失败)
    """

    return MainStationData.get_latest_tick(type, API_KEY, symbols)

def run_server():
    try:
        import sys
        import os

        print("启动 VVTR MCP 服务器...", file=sys.stderr)
        print(f"当前工作目录: {os.getcwd()}", file=sys.stderr)
        print(f"DATA_PATH: {os.environ.get('DATA_PATH')}", file=sys.stderr)

        # 验证 API_KEY 是否合法
        if not MainStationData.http_get(API_KEY):
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

