import os
from pathlib import Path
from typing import List, Union


class FolderSize:

    @staticmethod
    def get_folder_size_file(folder: Union[str, Path]) -> int:
        """
        使用os.walk递归计算文件夹大小

        Args:
            folder: 要计算大小的文件夹路径

        Returns:
            文件夹的总大小（字节）
        """
        folder_path = Path(folder)
        size = 0

        if folder_path.is_dir():
            for path, _, files in os.walk(folder_path):
                for file in files:
                    file_path = Path(path) / file
                    try:
                        size += file_path.stat().st_size
                    except (PermissionError, FileNotFoundError):
                        # 忽略无法访问的文件
                        continue
        else:
            size = folder_path.stat().st_size

        return size

    @staticmethod
    def get_folder_size_nio(path: Union[str, Path]) -> int:
        """
        使用pathlib计算文件夹大小（类似Java的NIO和Stream API）

        Args:
            path: 要计算大小的文件夹路径

        Returns:
            文件夹的总大小（字节）
        """
        folder_path = Path(path)
        size = 0

        try:
            # 递归遍历所有常规文件
            for file_path in folder_path.rglob('*'):
                if file_path.is_file():
                    try:
                        size += file_path.stat().st_size
                    except (PermissionError, FileNotFoundError):
                        # 忽略无法访问的文件
                        continue
        except Exception as e:
            print(f"计算文件夹大小时出错: {e}")

        return size

    @staticmethod
    def format_size(size: int) -> str:
        """
        格式化文件大小为可读形式

        Args:
            size: 文件大小（字节）

        Returns:
            格式化后的大小字符串，如 "1.25 MB"
        """
        units = ["字节", "KB", "MB", "GB", "TB"]
        unit_index = 0
        file_size = float(size)

        while file_size > 1024 and unit_index < len(units) - 1:
            file_size /= 1024
            unit_index += 1

        return f"{file_size:.2f} {units[unit_index]}"

    @staticmethod
    def get_data_count(size: int) -> int:
        """
        根据文件大小估算数据条数

        Args:
            size: 文件大小（字节）

        Returns:
            估算的数据条数
        """
        return size // 120

    @staticmethod
    def get_file_size(paths: List[Path]) -> int:
        """
        计算多个文件的总大小

        Args:
            paths: 文件路径列表

        Returns:
            文件的总大小（字节）
        """
        total_size = 0

        for path in paths:
            try:
                if path.is_file():
                    total_size += path.stat().st_size
            except Exception as e:
                print(f"获取文件大小时出错: {path} - {e}")

        return total_size


# 使用示例
if __name__ == "__main__":
    # 示例：计算当前目录大小
    current_dir = Path("")

    # 使用第一种方法计算
    size1 = FolderSize.get_folder_size_file(current_dir)
    print(f"方法1计算的文件夹大小: {FolderSize.format_size(size1)}")

    # 使用第二种方法计算
    size2 = FolderSize.get_folder_size_nio(current_dir)
    print(f"方法2计算的文件夹大小: {FolderSize.format_size(size2)}")

    # 估算数据条数
    data_count = FolderSize.get_data_count(size1)
    print(f"估算数据条数: {data_count}")