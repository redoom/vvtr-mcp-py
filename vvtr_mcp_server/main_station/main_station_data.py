import requests

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
                print(data)  # 这里打印出提取的data
                if data is None:
                    return False
                else:
                    return True
            else:
                print(f"请求失败，状态码: {req.status_code}")
                return False

        except Exception as e:
            print(f"发生错误: {e}")


if __name__ == "__main__":
    if MainStationData.http_get('12345'):
        print('合法')
    else:
        print('不合法')
