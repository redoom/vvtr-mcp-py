
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any

from util.csv_merger import CsvMerger

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# 返回数据类
class DataLabel:
    def __init__(self, data: str, next_index: int, remaining_paths: List[Path]):
        self.data = data
        self.next_index = next_index
        self.remaining_paths = remaining_paths


class DataBack:
    def __init__(self, data: str, remaining_paths: List[Path]):
        self.data = data
        self.remaining_paths = remaining_paths

class VvtrData:
    def __init__(self):
        pass

    def get_minute_data(self, paths: List[Path], next_index: int, count: int) -> DataLabel:
        """
        获取分钟数据

        Args:
            paths: 解析的文件路径
            next_index: 上一次的读取位置
            count: 一共需要的读取条数

        Returns:
            DataLabel: 包含数据、下一次的读取位置和剩余文件路径
        """
        all_content = ""  # 用于存储查询到的所有结果
        result_data = []  # 用于存储返回的结果
        total_lines_processed = 0  # 总的行数
        processed_path_index = 0  # 文件索引
        line = 0
        index = 0

        for i, path in enumerate(paths):
            data = CsvMerger.parse_csv_without_header_as_string(path)
            all_content += data
            lines = CsvMerger.count_lines(data)
            line = lines
            total_lines_processed += lines
            processed_path_index = i

            if i == 0 and (total_lines_processed - next_index) > count:
                break
            elif total_lines_processed > count:
                break

        # 将合并后的内容按行分割
        all_lines = all_content.split('\n')
        actual_limit = min(count, len(all_lines))

        result_data = all_lines[next_index:next_index + actual_limit]
        result_str = '\n'.join(result_data)

        # 计算下一次索引
        if count < len(all_lines):
            index = next_index + actual_limit + line - total_lines_processed
            if index < 0:
                lines = CsvMerger.count_lines(CsvMerger.parse_csv_without_header_as_string(paths[0]))
                index = lines + index

        # 剩余的路径
        remaining_paths = paths[processed_path_index:]

        return DataLabel(result_str, index, remaining_paths)

    def get_day_data(self, filter_data: str, offset: int, limit: int) -> str:
        """
        获取日线数据

        Args:
            filter_data: 过滤后的数据，每条数据以\n结尾
            offset: 起始位置（第几条开始）
            limit: 结束位置（第几条结束）

        Returns:
            str: 指定范围内的数据
        """
        # 将数据按行分割
        lines = filter_data.split('\n')

        # 设置默认值
        start_index = max(offset, 0)
        end_index = min(start_index + limit, len(lines)) if limit > 0 else len(lines)

        # 检查数据量是否超过 1000 条
        if end_index - start_index > 1000:
            end_index = start_index + 1000

        # 提取指定范围的数据
        result = '\n'.join(lines[start_index:end_index])
        if result and not result.endswith('\n'):
            result += '\n'

        return result

    def get_day_data_with_paths(self, paths: List[Path], symbol: str, symbol_index: int,
                                start_time: str, end_time: str, bob_index: int) -> DataBack:
        """
        获取日线数据

        Args:
            paths: 文件路径列表
            symbol: 符号
            symbol_index: 符号在CSV中的索引
            start_time: 开始时间(yyyy-MM-dd)
            end_time: 结束时间(yyyy-MM-dd)
            bob_index: 时间字段索引

        Returns:
            DataBack: 包含过滤后的数据和剩余文件路径
        """
        processed_path_index = 0
        result_lines = []

        # 解析日期
        start_date = None
        end_date = None
        try:
            if start_time:
                start_date = datetime.strptime(start_time, "%Y-%m-%d").date()
            if end_time:
                end_date = datetime.strptime(end_time, "%Y-%m-%d").date()
        except Exception as e:
            logger.error(f"日期时间解析失败，将使用字符串比较: {str(e)}")

        for i, path in enumerate(paths):
            processed_path_index = i
            data = CsvMerger.parse_csv_without_header_as_string(path)
            lines = data.split('\n')

            for line in lines:
                if not line.strip():
                    continue

                single = line.split(',')
                if len(single) <= bob_index + 1:
                    continue

                # 时间校验
                try:
                    start_data_time = single[bob_index].split(' ')[0]
                    end_data_time = single[bob_index + 1].split(' ')[0]

                    start_date_local_time = datetime.strptime(start_data_time, "%Y-%m-%d").date()
                    end_date_local_time = datetime.strptime(end_data_time, "%Y-%m-%d").date()

                    if start_date and end_date and symbol:
                        if (not start_date_local_time > end_date and
                                not end_date_local_time < start_date and
                                single[symbol_index] == symbol):
                            result_lines.append(line)
                    elif symbol:
                        if single[symbol_index] == symbol:
                            result_lines.append(line)
                    else:
                        result_lines.append(line)
                except Exception as e:
                    logger.error(f"处理行时出错: {str(e)}")

        # 剩余的路径
        remaining_paths = paths[processed_path_index + 1:]
        result_str = '\n'.join(result_lines)
        if result_str and not result_str.endswith('\n'):
            result_str += '\n'

        return DataBack(result_str, remaining_paths)

    def get_min_data(self, paths: List[Path], start_time: str, end_time: str, bob_index: int) -> DataBack:
        """
        获取分钟数据

        Args:
            paths: 文件路径列表
            start_time: 开始时间(yyyy-MM-dd HH:mm:ss)
            end_time: 结束时间(yyyy-MM-dd HH:mm:ss)
            bob_index: 时间字段索引

        Returns:
            DataBack: 包含过滤后的数据和剩余文件路径
        """
        processed_path_index = 0
        result_lines = []
        total_line = 0

        # 解析日期时间
        start_datetime = None
        end_datetime = None
        try:
            if start_time:
                start_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            if end_time:
                end_datetime = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"日期时间解析失败，将使用字符串比较: {str(e)}")

        for i, path in enumerate(paths):
            processed_path_index = i
            data = CsvMerger.parse_csv_without_header_as_string(path)
            data_lines = CsvMerger.count_lines(data)
            total_line += data_lines

            if total_line > 1000 and i > 0:
                break

            lines = data.split('\n')
            for line in lines:
                if not line.strip():
                    continue

                single = line.split(',')
                if len(single) <= bob_index + 1:
                    continue

                # 时间校验
                try:
                    start_data_time = single[bob_index].split('+')[0]
                    end_data_time = single[bob_index + 1].split('+')[0]

                    start_date_local_time = datetime.strptime(start_data_time, "%Y-%m-%d %H:%M:%S")
                    end_date_local_time = datetime.strptime(end_data_time, "%Y-%m-%d %H:%M:%S")

                    if start_datetime and end_datetime:
                        if (not start_date_local_time > end_datetime and
                                not end_date_local_time < start_datetime):
                            result_lines.append(line)
                    else:
                        result_lines.append(line)
                except Exception as e:
                    logger.error(f"处理行时出错: {str(e)}")

        # 剩余的路径
        remaining_paths = paths[processed_path_index + 1:]
        result_str = '\n'.join(result_lines)
        if result_str and not result_str.endswith('\n'):
            result_str += '\n'

        return DataBack(result_str, remaining_paths)

    def get_min500_data(self, paths: List[Path], start_time: str, end_time: str, bob_index: int) -> DataBack:
        """
        获取分钟数据，限制500条

        Args:
            paths: 文件路径列表
            start_time: 开始时间(yyyy-MM-dd HH:mm:ss)
            end_time: 结束时间(yyyy-MM-dd HH:mm:ss)
            bob_index: 时间字段索引

        Returns:
            DataBack: 包含过滤后的数据和剩余文件路径
        """
        # 与get_min_data类似，但限制500条
        processed_path_index = 0
        result_lines = []
        total_line = 0

        # 解析日期时间
        start_datetime = None
        end_datetime = None
        try:
            if start_time:
                start_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            if end_time:
                end_datetime = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"日期时间解析失败，将使用字符串比较: {str(e)}")

        for i, path in enumerate(paths):
            processed_path_index = i
            data = CsvMerger.parse_csv_without_header_as_string(path)
            data_lines = CsvMerger.count_lines(data)
            total_line += data_lines

            if total_line > 500 and i > 0:  # 这里限制为500
                break

            lines = data.split('\n')
            for line in lines:
                if not line.strip():
                    continue

                single = line.split(',')
                if len(single) <= bob_index + 1:
                    continue

                # 时间校验
                try:
                    start_data_time = single[bob_index].split('+')[0]
                    end_data_time = single[bob_index + 1].split('+')[0]

                    start_date_local_time = datetime.strptime(start_data_time, "%Y-%m-%d %H:%M:%S")
                    end_date_local_time = datetime.strptime(end_data_time, "%Y-%m-%d %H:%M:%S")

                    if start_datetime and end_datetime:
                        if (not start_date_local_time > end_datetime and
                                not end_date_local_time < start_datetime):
                            result_lines.append(line)
                    else:
                        result_lines.append(line)
                except Exception as e:
                    logger.error(f"处理行时出错: {str(e)}")

        # 剩余的路径
        remaining_paths = paths[processed_path_index + 1:]
        result_str = '\n'.join(result_lines)
        if result_str and not result_str.endswith('\n'):
            result_str += '\n'

        return DataBack(result_str, remaining_paths)

    def get_tick_data(self, paths: List[Path], start_time: str, end_time: str,
                      create_time_index: int, next_index: int, count: int) -> DataLabel:
        """
        获取Tick数据

        Args:
            paths: 文件路径列表
            start_time: 开始时间(yyyy-MM-dd HH:mm:ss)
            end_time: 结束时间(yyyy-MM-dd HH:mm:ss)
            create_time_index: 创建时间在CSV中的索引
            next_index: 上一次的读取位置
            count: 需要读取的条数

        Returns:
            DataLabel: 包含数据、下一次的读取位置和剩余文件路径
        """
        if count > 180:
            count = 180

        result_lines = []
        total_line = 0

        # 解析日期时间
        start_datetime = None
        end_datetime = None
        index = 0
        current_line = 0

        try:
            if start_time:
                start_datetime = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            if end_time:
                end_datetime = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"日期时间解析失败: {str(e)}")

        processed_path_index = 0
        for i, path in enumerate(paths):
            processed_path_index = i
            current_line = CsvMerger.count_lines(CsvMerger.parse_csv_without_header_as_string(paths[i]))

            try:
                with open(path, 'r', encoding='utf-8') as file:
                    # 获取并跳过标题行
                    header_line = file.readline()
                    if header_line:
                        result_lines.append(header_line.rstrip())

                    # 读取和处理数据行
                    for line in file:
                        line = line.rstrip()
                        if not line:
                            continue

                        fields = self.parse_csv_line(line)
                        total_line += 1

                        # 确保创建时间索引在范围内
                        if len(fields) <= create_time_index or create_time_index < 0:
                            logger.warning("警告: 创建时间索引超出范围")
                            continue

                        # 提取日期时间
                        created_at_field = fields[create_time_index]
                        record_time = self.extract_date_time(created_at_field)

                        # 时间筛选
                        if self.should_include_record(record_time, start_datetime, end_datetime):
                            result_lines.append(line)

                        if total_line - next_index > count and i > 0:
                            break
            except Exception as e:
                logger.error(f"读取文件失败: {str(e)}")

        # 每一行数据
        all_lines = result_lines
        # 实际限制
        actual_limit = min(count, len(all_lines))

        # 提取结果
        if next_index < len(all_lines):
            result_data = all_lines[next_index:next_index + actual_limit]
            result_str = '\n'.join(result_data)
        else:
            result_str = ""

        # 计算下一次索引
        if count < len(all_lines):
            index = next_index + actual_limit + current_line - len(all_lines)
            if index < 0:
                lines = CsvMerger.count_lines(CsvMerger.parse_csv_without_header_as_string(paths[0]))
                index = lines + index

        # 剩余的路径
        remaining_paths = paths[processed_path_index:]

        return DataLabel(result_str, index, remaining_paths)

    def parse_csv_line(self, line: str) -> List[str]:
        """解析CSV行，正确处理引号内的内容"""
        fields = []
        in_quotes = False
        field = ""

        for i in range(len(line)):
            c = line[i]
            if c == '"':
                in_quotes = not in_quotes
            elif c == ',' and not in_quotes:
                fields.append(field)
                field = ""
            else:
                field += c

        fields.append(field)  # 添加最后一个字段
        return fields

    def extract_date_time(self, created_at_field: str) -> Optional[datetime]:
        """从字段中提取日期时间"""
        try:
            # 处理可能的格式，例如："2025-04-28 09:15:00+0800"
            date_str = created_at_field
            if "created_at" in date_str:
                date_str = date_str[date_str.index(":") + 1:].strip()

            # 移除时区信息
            if "+" in date_str:
                date_str = date_str[:date_str.index("+")]

            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"提取日期时间失败: {str(e)}")
            return None

    def should_include_record(self, record_time: Optional[datetime],
                              start_time: Optional[datetime],
                              end_time: Optional[datetime]) -> bool:
        """检查记录是否在指定的时间范围内"""
        if record_time is None:
            return True  # 如果无法解析时间，则包含该记录
        if start_time is None and end_time is None:
            return True  # 如果没有指定时间范围，则包含所有记录

        after_start = start_time is None or not record_time < start_time
        before_end = end_time is None or not record_time > end_time

        return after_start and before_end