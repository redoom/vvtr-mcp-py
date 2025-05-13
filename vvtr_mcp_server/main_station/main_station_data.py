import requests  # 确保添加导入语句


class MainStationData:
    # 将ROOT_PATH改为类变量
    ROOT_PATH = 'https://api.vvtr.com/v1'

    @staticmethod
    def http_get(apiKey):
        params = {'apiKey': apiKey}
        try:
            req = requests.get(MainStationData.ROOT_PATH + '/userPermissions/getUserByApiKey',
                               params=params,
                               timeout=10)

            if req.status_code == 200:
                # 直接解析JSON并提取data字段
                response_dict = req.json()
                data = response_dict["data"]
                if data is None:
                    return False
                else:
                    return True
            else:
                print(f"请求失败，状态码: {req.status_code}")
                return False

        except Exception as e:
            print(f"发生错误: {e}")

    @staticmethod
    def get_symbol(type: str, apikey: str) -> str | None:
        params = {'type': type, 'apiKey': apikey}
        try:
            req = requests.get(MainStationData.ROOT_PATH + '/symbols',
                               params=params)

            if req.status_code == 200:
                # 直接解析JSON并提取data字段
                response_dict = req.json()
                data = response_dict["data"]

                # 将JSON数据转换为CSV格式
                csv_lines = []
                for item in data:
                    csv_line = f"{item['symbol']},{item['exchange']},{item['name']},{item['delistedDate']},{item['listedDate']},{item['type']}"
                    csv_lines.append(csv_line)

                # 如果需要返回所有行拼接的字符串
                return "\n".join(csv_lines)
            else:
                print(f"请求失败，状态码: {req.status_code}")
                return None

        except Exception as e:
            print(f"发生错误: {e}")
            return None

    @staticmethod
    def count_lines_in_string(data_string):
        if not data_string:
            return 0
        lines = data_string.strip().split('\n')
        count = len(lines)
        print(f"数据中共有 {count} 条记录")
        return count

    @staticmethod
    def cut_data(data: str, start: int, end: int) -> str:
        """
        从多行数据中切片获取指定范围的数据

        参数:
            data: 字符串，包含多行数据，每行以 \n 分隔
            start: 开始索引（包含）
            end: 结束索引（不包含）

        返回:
            切片后的数据字符串，每行仍以 \n 分隔
        """
        if not data:
            return ""

        # 将数据分割成行列表
        lines = data.strip().split('\n')
        total_lines = len(lines)

        # 处理索引超出范围的情况
        if start < 0:
            start = 0
        if end > total_lines:
            end = total_lines
        if start >= total_lines or end <= 0 or start >= end:
            return ""

        # 切片获取指定范围的行
        selected_lines = lines[start:end]

        # 将结果重新连接成字符串
        return '\n'.join(selected_lines)

    @staticmethod
    def get_history_kline(symbols: str, interval: str, type: str, apikey: str,
                          from_date: str, to_date: str, adjust: bool = False,
                          limit: int = 2000, cursor_token: str = None) -> tuple[str | None, bool, str]:
        """
        获取历史K线数据

        Args:
            symbols: 证券代码,多个代码用逗号分隔,填"*"则提交分类下的全部symbol
            interval: 周期(如1m, 5m, 15m, 30m, 1h, 4h, 1d等)
            type: 产品类型
            apikey: 您的apiKey
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
        params = {
            'symbols': symbols,
            'interval': interval,
            'type': type,
            'apiKey': apikey,
            'from': from_date,
            'to': to_date
        }

        # 添加可选参数
        if adjust:
            params['adjust'] = adjust
        if limit != 2000:  # 只有在非默认值时添加
            params['limit'] = limit
        if cursor_token:
            params['cursorToken'] = cursor_token

        try:
            req = requests.get(MainStationData.ROOT_PATH + '/kline/history', params=params)

            if req.status_code == 200:
                response_dict = req.json()

                if response_dict["code"] != 200:
                    print(f"API返回错误: {response_dict['msg']}")
                    return None, False, ''

                # 获取记录列表
                records = response_dict["data"]["records"]

                # 获取分页状态
                has_next = response_dict["data"]["hasNext"]
                next_cursor_token = response_dict["data"].get("nextCursorToken", '')
                if next_cursor_token is None:  # 处理None值转为空字符串
                    next_cursor_token = ''

                # 如果没有数据，返回None
                if not records:
                    return None, has_next, next_cursor_token

                # 创建CSV格式的数据
                # 添加表头
                header = "id,symbol,interval,open,high,close,low,amount,volume,position,bob,eob,type,sequence"
                csv_lines = [header]

                # 添加数据行
                for item in records:
                    csv_line = (f"{item['id']},{item['symbol']},{item['interval']},"
                                f"{item['open']},{item['high']},{item['close']},{item['low']},"
                                f"{item['amount']},{item['volume']},{item['position']},"
                                f"{item['bob']},{item['eob']},{item['type']},{item['sequence']}")
                    csv_lines.append(csv_line)

                # 返回所有行拼接的字符串、hasNext状态和nextCursorToken
                return "\n".join(csv_lines), has_next, next_cursor_token
            else:
                print(f"请求失败，状态码: {req.status_code}")
                return None, False, ''

        except Exception as e:
            print(f"发生错误: {e}")
            return None, False, ''

    @staticmethod
    def get_current_kline(type: str, apikey: str, symbols: str = None) -> str | None:
        """
        获取最新分钟K线数据

        Args:
            type: 产品类型，可填写多个，用","分隔。各个type的权限需独立获取。
            apikey: 您的apiKey
            symbols: 证券代码，用","分隔，多个type的symbol用";"分隔，顺序务必与type保持一致。

        Returns:
            返回CSV格式的最新分钟K线数据或None(如果请求失败)
        """
        params = {
            'type': type,
            'apiKey': apikey
        }

        # 只有在提供symbols参数时才添加
        if symbols:
            params['symbols'] = symbols

        try:
            req = requests.get(MainStationData.ROOT_PATH + '/kline/current', params=params)

            if req.status_code == 200:
                response_dict = req.json()

                if response_dict["code"] != 200:
                    print(f"API返回错误: {response_dict['msg']}")
                    return None

                # 获取记录列表
                records = response_dict["data"]

                # 如果没有数据，返回None
                if not records:
                    return None

                # 创建CSV格式的数据
                # 添加表头
                header = "symbol,frequency,open,high,close,low,amount,volume,position,bob,eob,type"
                csv_lines = [header]

                # 添加数据行
                for item in records:
                    frequency = item.get('frequency', '')
                    if frequency is None:  # 处理None值
                        frequency = ''

                    csv_line = (f"{item['symbol']},{frequency},{item['open']},"
                                f"{item['high']},{item['close']},{item['low']},"
                                f"{item['amount']},{item['volume']},{item['position']},"
                                f"{item['bob']},{item['eob']},{item['type']}")
                    csv_lines.append(csv_line)

                # 返回所有行拼接的字符串
                return "\n".join(csv_lines)
            else:
                print(f"请求失败，状态码: {req.status_code}")
                return None

        except Exception as e:
            print(f"发生错误: {e}")
            return None

    @staticmethod
    def get_latest_tick(type: str, apikey: str, symbols: str = None) -> str | None:
        """
        获取最新tick数据

        Args:
            type: 产品类型，可填写多个，用","分隔。各个type的权限需独立获取。
            apikey: 您的apiKey
            symbols: 证券代码，用","分隔，多个type的symbol用";"分隔，顺序务必与type保持一致。

        Returns:
            返回CSV格式的最新tick数据或None(如果请求失败)
        """
        params = {
            'type': type,
            'apiKey': apikey
        }

        # 只有在提供symbols参数时才添加
        if symbols:
            params['symbols'] = symbols

        try:
            req = requests.get(MainStationData.ROOT_PATH + '/briefs', params=params)

            if req.status_code == 200:
                response_dict = req.json()

                if response_dict["code"] != 200:
                    print(f"API返回错误: {response_dict['msg']}")
                    return None

                # 获取记录列表
                records = response_dict["data"]

                # 如果没有数据，返回None
                if not records:
                    return None

                # 创建主CSV表头
                main_header = "symbol,open,high,low,price,cumVolume,cumAmount,cumPosition,tradeType,lastVolume,lastAmount,createdAt"

                # 创建quotes表头 (最多10档)
                quotes_headers = []
                for i in range(10):  # 最多10档
                    quotes_headers.append(f"bidP{i + 1},bidV{i + 1},askP{i + 1},askV{i + 1}")

                # 合并所有表头
                header = main_header + "," + ",".join(quotes_headers) + ",iopv"
                csv_lines = [header]

                # 添加数据行
                for item in records:
                    # 处理主要字段
                    main_values = (f"{item.get('symbol', '')},"
                                   f"{item.get('open', 0)},"
                                   f"{item.get('high', 0)},"
                                   f"{item.get('low', 0)},"
                                   f"{item.get('price', 0)},"
                                   f"{item.get('cumVolume', 0)},"
                                   f"{item.get('cumAmount', 0)},"
                                   f"{item.get('cumPosition', 0)},"
                                   f"{item.get('tradeType', 0)},"
                                   f"{item.get('lastVolume', 0)},"
                                   f"{item.get('lastAmount', 0)},"
                                   f"{item.get('createdAt', '')}")

                    # 处理quotes字段
                    quotes = item.get('quotes', [])
                    quotes_values = []

                    # 填充所有可能的档位
                    for i in range(10):
                        if i < len(quotes):
                            quote = quotes[i]
                            quotes_values.append(
                                f"{quote.get('bidP', 0)},{quote.get('bidV', 0)},{quote.get('askP', 0)},{quote.get('askV', 0)}")
                        else:
                            quotes_values.append("0,0,0,0")  # 缺失的档位填充0

                    # 处理iopv字段（基金特有）
                    iopv = item.get('iopv', 0)

                    # 合并所有字段
                    csv_line = main_values + "," + ",".join(quotes_values) + f",{iopv}"
                    csv_lines.append(csv_line)

                # 返回所有行拼接的字符串
                return "\n".join(csv_lines)
            else:
                print(f"请求失败，状态码: {req.status_code}")
                return None

        except Exception as e:
            print(f"发生错误: {e}")
            return None

if __name__ == "__main__":
    print(MainStationData.get_history_kline('600000', '1m', '11', '7cb46b2c-d8c8-46e8-9233-536346110b31', '2025-04-01', '2025-04-03', limit=1))
