import os
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def initialize_folders():
    # 从环境变量获取基础路径，如果没有设置则使用默认值
    base_path = os.environ.get("USER_DATA_PATH", 'C:\\data')

    # 金融产品类型
    folders = [
        "11",  # A股
        "14",  # 期货
        "12",  # 基金
        "16",  # 指数
        "21",  # 美股
        "22",  # 美股期权
        "31"  # 加密币
    ]

    # 时间周期类型
    time_types = [
        "1d", "1m", "15m", "tick"
    ]

    # 创建文件夹结构
    for folder in folders:
        folder_path = os.path.join(base_path, folder)
        for time_type in time_types:
            type_dir = os.path.join(folder_path, time_type)
            if not os.path.exists(type_dir):
                try:
                    os.makedirs(type_dir)
                    logger.info(f"Created folder: {type_dir}")
                except Exception as e:
                    logger.error(f"Failed to create folder: {type_dir}. Error: {str(e)}")
            else:
                logger.info(f"Folder already exists: {type_dir}")