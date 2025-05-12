import os
import csv
from pathlib import Path
from typing import List


class CsvMerger:
    # 指定要扫描的目录
    ROOT = os.environ.get("USER_DATA_PATH", ".")
    # ROOT = r"D:\\data\\"
    @staticmethod
    def main(args=None):
        """
        程序入口：遍历 ROOT 下所有 .csv 文件，
        跳过每个文件的第一行表头，
        将所有记录拼接成一个大字符串，并以 '\n' 分隔，直接打印全部。
        """
        try:
            root_dir = Path(CsvMerger.ROOT)
            csv_files = CsvMerger.find_all_csv_files(root_dir)
            # 直接把所有数据一次性打印出来
            print(csv_files)
        except Exception as e:
            print(f"❌ 合并 CSV 失败： {str(e)}")
            import traceback
            traceback.print_exc()

    @staticmethod
    def merge_csv_files(root_dir: Path) -> str:
        """
        遍历目录下所有 .csv 文件，合并其数据为一个字符串
        自动跳过每个文件的第一行
        """
        result = []
        for path in Path(root_dir).rglob("*.csv"):
            result.extend(CsvMerger.parse_file_as_stream(path))
        return "\n".join(result)

    @staticmethod
    def parse_file_as_stream(csv_path: Path) -> List[str]:
        """
        把单个 CSV 文件解析为字符串列表，
        每条记录转成 "字段1,字段2,..." 的字符串，
        并自动跳过第一行表头
        """
        try:
            result = []
            with open(csv_path, 'r', encoding='utf-8') as f:
                csv_reader = csv.reader(f)
                next(csv_reader)  # 跳过表头
                for record in csv_reader:
                    result.append(",".join(record))
            return result
        except Exception as e:
            print(f"解析文件 {csv_path} 时出错: {str(e)}")
            return []

    @staticmethod
    def find_all_csv_files(root_dir: Path) -> List[Path]:
        """
        在目录中查找所有CSV文件
        """
        return list(root_dir.rglob("*.csv"))

    @staticmethod
    def find_all_csv_files_with_date_range(root_dir: Path, start_date: str, end_date: str) -> List[Path]:
        """
        根据指定的日期范围在目录中查找所有CSV文件。

        Args:
            root_dir: 要搜索的根目录
            start_date: 过滤的开始日期（包含），格式为 "yyyyMMdd"
            end_date: 过滤的结束日期（包含），格式为 "yyyyMMdd"
        Returns:
            表示过滤后的CSV文件的Path对象列表
        """
        return_all = start_date == "00000000" and end_date == "99999999"
        result = []

        for path in root_dir.rglob("*.csv"):
            try:
                # 获取文件所在的目录
                parent = path.parent

                # 如果父目录为空，无法过滤
                if parent is None:
                    continue

                # 获取父目录的名称（这应该是日期格式，例如 20200101）
                date_str = parent.name

                if root_dir.name == date_str:
                    result.append(path)
                    continue

                # 确保日期格式正确
                if len(date_str) != 8:
                    continue

                if return_all:
                    result.append(path)
                    continue

                # 使用字符串比较判断日期是否在范围内
                if start_date <= date_str <= end_date:
                    result.append(path)
            except Exception:
                continue

        return result

    @staticmethod
    def find_all_csv_files_with_date_range_and_symbol(root_dir: Path, start_date: str, end_date: str, symbol: str) -> \
    List[Path]:
        """
        根据指定的日期范围和产品代码在目录中查找所有CSV文件。

        Args:
            root_dir: 要搜索的根目录
            start_date: 过滤的开始日期（包含），格式为 "yyyyMMdd"
            end_date: 过滤的结束日期（包含），格式为 "yyyyMMdd"
            symbol: 过滤产品代码
        Returns:
            表示过滤后的CSV文件的Path对象列表
        """
        return_all = start_date == "00000000" and end_date == "99999999"
        result = []

        for path in root_dir.rglob("*.csv"):
            try:
                # 获取文件名（不含扩展名）
                file_name = path.name
                file = file_name[:file_name.rfind('.')]

                if not symbol:
                    pass  # 如果symbol为空，不根据symbol过滤
                elif file != symbol:
                    continue  # 如果文件名不匹配symbol，跳过

                # 获取文件所在的目录
                parent = path.parent

                # 如果父目录为空，无法过滤
                if parent is None:
                    continue

                # 获取父目录的名称（这应该是日期格式，例如 20200101）
                date_str = parent.name

                if root_dir.name == date_str:
                    result.append(path)
                    continue

                # 确保日期格式正确
                if len(date_str) != 8:
                    continue

                if return_all:
                    result.append(path)
                    continue

                # 使用字符串比较判断日期是否在范围内
                if start_date <= date_str <= end_date:
                    result.append(path)
            except Exception:
                continue

        return result

    @staticmethod
    def parse_csv_without_header_as_string(path: Path) -> str:
        """
        解析CSV文件（跳过表头）并将所有数据行连接成一个字符串

        Args:
            path: CSV文件路径
        Returns:
            包含所有数据行（不含表头）的字符串，行与行之间用\n分隔
        """
        result = []
        try:
            with open(path, 'r') as f:
                lines = f.readlines()

                # 跳过第一行（表头）
                for line in lines[1:]:
                    result.append(line.rstrip('\n'))

                # 在循环结束后添加
                if result:
                    result.append("")
        except Exception as e:
            print(f"读取CSV文件时出错: {str(e)}")

        return "\n".join(result)

    @staticmethod
    def parse_multiple_csv_files_without_header(paths: List[Path]) -> str:
        """
        如果需要处理多个CSV文件

        Args:
            paths: CSV文件路径列表
        Returns:
            包含所有文件数据的字符串（不含表头）
        """
        all_data = []
        first_file = True

        for path in paths:
            file_data = CsvMerger.parse_csv_without_header_as_string(path)

            if file_data:
                if first_file:
                    all_data.append(file_data)
                    first_file = False
                else:
                    # 在不同文件之间添加换行符
                    all_data.append(file_data)

        return "\n".join(all_data)

    @staticmethod
    def count_lines(text: str) -> int:
        """
        统计一共有多少行数据

        Args:
            text: 需要统计的文本
        Returns:
            返回行数
        """
        if not text:
            return 0

        # 检查文本是否以换行符结尾
        if text.endswith("\n"):
            # 如果以换行符结尾，减去多余的一行
            return len(text.split("\n")) - 1
        else:
            # 如果不以换行符结尾，直接返回分割结果
            return len(text.split("\n"))

    @staticmethod
    def filter_data(data: str, symbol: str) -> str:
        """
        1d使用，读取到所有的数据获取指定

        Args:
            data: 数据
            symbol: 种类代码用于过滤
        Returns:
            过滤后的数据
        """
        if not data or not symbol:
            return ""

        lines = data.split("\n")

        # 确保至少有一行（表头）
        if not lines:
            return ""

        # 添加表头到结果
        header = lines[0]
        result = [header]

        # 解析表头，找到symbol所在的列索引
        header_fields = header.split(",")
        symbol_index = -1

        for i, field in enumerate(header_fields):
            if field.strip().lower() == "symbol":
                symbol_index = i
                break

        # 如果没有找到symbol列，返回只有表头的结果
        if symbol_index == -1:
            return header

        # 遍历数据行（跳过表头）
        for i in range(1, len(lines)):
            line = lines[i]
            fields = line.split(",")

            # 确保行有足够的字段
            if len(fields) > symbol_index:
                # 检查symbol列是否与给定的symbol匹配
                if fields[symbol_index] == symbol:
                    # 将匹配的行添加到结果中
                    result.append(line)

        return "\n".join(result)

    @staticmethod
    def get_bob_index(path: Path) -> int:
        """
        获取bob列的索引
        """
        try:
            with open(path, 'r') as f:
                line = f.readline().strip()
                str_fields = line.split(",")
                for i, field in enumerate(str_fields):
                    if field.strip().lower() == "bob":
                        return i
                return -1
        except Exception as e:
            print(f"读取CSV文件时出错: {str(e)}")
            return -1

    @staticmethod
    def get_symbol_index(path: Path) -> int:
        """
        获取symbol列的索引
        """
        try:
            with open(path, 'r') as f:
                line = f.readline().strip()
                str_fields = line.split(",")
                for i, field in enumerate(str_fields):
                    if field.strip().lower() == "symbol":
                        return i
                return -1
        except Exception as e:
            print(f"读取CSV文件时出错: {str(e)}")
            return -1

    @staticmethod
    def get_create_time_index(path: Path) -> int:
        """
        获取created_at列的索引
        """
        try:
            with open(path, 'r') as f:
                line = f.readline().strip()
                str_fields = line.split(",")
                for i, field in enumerate(str_fields):
                    if field.strip().lower() == "created_at":
                        return i
                return -1
        except Exception as e:
            print(f"读取CSV文件时出错: {str(e)}")
            return -1


# 如果直接运行此脚本
if __name__ == "__main__":
    CsvMerger.main()