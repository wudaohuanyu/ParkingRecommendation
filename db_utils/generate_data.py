import pandas as pd
import numpy as np
import requests
import random
import time


def fetch_parking_lots(city, api_key):
    """
    使用高德地图API获取不同类型的停车场的位置信息。

    参数:
    city (str): 查询的城市名称或城市行政区划代码
    api_key (str): 高德地图API的秘钥

    返回:
    parking_lots (list): 停车场位置信息的列表，每一项是字典，包括名称、经纬度和类型。
    """
    parking_lots = []

    # 定义不同类型的停车场类型代码和对应的权重
    type_codes = {
        '商场': (['060101', '060102'], 3),  # 大型购物中心，综合商场，权重为3
        '火车站': (['150202'], 1),  # 火车站停车场，权重为1
        '机场': (['150101'], 1),  # 机场停车场，权重为1
        '停车场': (['150900'], 5),  # 停车场，权重为5
    }

    # 按照权重构建请求顺序
    weighted_requests = []
    for parking_type, (codes, weight) in type_codes.items():
        for _ in range(weight):
            weighted_requests.append((parking_type, codes))

    # 打乱请求顺序，避免每次顺序相同
    random.shuffle(weighted_requests)

    # 遍历类型代码进行多次请求
    for parking_type, codes in weighted_requests:
        for code in codes:
            try:
                url = (f"https://restapi.amap.com/v3/place/text?key={api_key}"
                       f"&city={city}&types={code}&offset=20&page=1&extensions=base")

                response = requests.get(url)
                response.raise_for_status()  # 检查请求是否成功

                data = response.json()

                if data.get('status') == '1':
                    # 遍历返回的停车场信息
                    for poi in data.get('pois', []):
                        parking_lots.append({
                            'name': poi.get('name', '未知停车场'),
                            'location': poi.get('location', '0,0'),
                            'typecode': poi.get('typecode', '未知代码'),  # 获取 typecode 字段
                            'type': parking_type  # 根据请求类型标记停车场类型
                        })
                else:
                    print(f"Error fetching parking lots for type {code}: {data.get('info', 'Unknown error')}")
            except requests.exceptions.RequestException as e:
                print(f"Request failed for type {code}: {e}")

            time.sleep(0.5)

    return parking_lots


def get_parking_type(parking_name, parking_type_code):
    """
    根据停车场名称和类型代码判断停车场类型，返回类型字符串。

    参数:
    parking_name (str): 停车场名称
    parking_type_code (str): 停车场的类型代码

    返回:
    str: 停车场类型
    """
    print("parking_name:", parking_name)
    # 如果typecode是150900，表示这是停车场的类型代码
    if parking_type_code == "150900":
        # 使用停车场名称来判断是地上还是地下停车场
        if '地下' in parking_name:
            return "地下停车场"
        elif '地面' in parking_name:
            return "地面停车场"
        else:
            return "未知停车场"  # 如果无法识别，可以返回"未知停车场"

    # 根据类型代码匹配火车站或机场等交通枢纽
    if parking_type_code.startswith("150202") or parking_type_code.startswith("150101"):
        return "交通枢纽"  # 火车站或机场

    # 根据类型代码匹配商场类型
    elif parking_type_code.startswith("0601"):
        return "商场"

    # 默认返回“其他”
    else:
        return "其他"


def generate_parking_data(num_users, num_parking_spots, min_ratings_per_user, max_ratings_per_user, parking_lots,
                          seed=42):
    """
    生成停车位数据和用户评分数据，并保存为CSV文件。

    参数:
    num_users (int): 用户数量
    num_parking_spots (int): 停车位数量
    min_ratings_per_user (int): 每个用户评分的最少停车位数量
    max_ratings_per_user (int): 每个用户评分的最多停车位数量
    parking_lots (list): 从API获取的停车场位置信息列表
    seed (int): 随机种子，默认值为42

    返回:
    parking_spots (DataFrame): 停车位信息数据DataFrame
    ratings_df (DataFrame): 用户评分数据DataFrame
    """
    np.random.seed(seed)

    # 生成停车位数据
    parking_data = {
        'ID': [],
        'Driving Distance (meters)': [],
        'Walking Distance (meters)': [],
        'Time to Find Parking (minutes)': [],
        'Parking Space Size (0-10)': [],
        'Parking Difficulty': [],
        'Near Elevator': [],
        'Has Surveillance': [],
        'Parking Fee (CNY/hour)': [],
        'Parking Type': [],
        'Longitude': [],
        'Latitude': []
    }

    for i in range(num_parking_spots):
        parking_lot = random.choice(parking_lots)
        parking_name = parking_lot['name']

        # 检查location格式
        location = parking_lot.get('location', '0,0')
        try:
            longitude, latitude = map(float, location.split(','))
        except ValueError:
            print(f"Warning: Invalid location format for {parking_name}, defaulting to (0, 0)")
            longitude, latitude = 0.0, 0.0

        parking_type_code = parking_lot['typecode']  # 传递 typecode 而不是 'type'

        # 获取停车场类型
        parking_type = get_parking_type(parking_name, parking_type_code)

        # 根据停车场类型生成不同范围的停车位数据
        if parking_type == "交通枢纽":
            driving_distance = np.random.randint(800, 2000)
            walking_distance = np.random.randint(300, 1000)
            time_to_find_parking = np.random.randint(10, 20)
            parking_space_size = np.random.randint(4, 7)
            parking_fee = round(np.random.uniform(10, 20), 2)
            near_elevator = np.random.choice(['是', '否'], p=[0.3, 0.7])
            has_surveillance = np.random.choice(['是', '否'], p=[0.8, 0.2])
            parking_difficulty = '困难'

        elif parking_type == "商场":
            driving_distance = np.random.randint(300, 1200)
            walking_distance = np.random.randint(200, 800)
            time_to_find_parking = np.random.randint(5, 15)
            parking_space_size = np.random.randint(6, 8)
            parking_fee = round(np.random.uniform(8, 15), 2)
            near_elevator = np.random.choice(['是', '否'], p=[0.5, 0.5])
            has_surveillance = np.random.choice(['是', '否'], p=[0.7, 0.3])
            parking_difficulty = np.random.choice(['中等', '困难'])

        elif parking_type == "地面停车场":
            driving_distance = np.random.randint(50, 500)
            walking_distance = np.random.randint(10, 400)
            time_to_find_parking = np.random.randint(1, 5)
            parking_space_size = np.random.randint(9, 10)
            parking_fee = round(np.random.uniform(2, 6), 2)
            near_elevator = '否'
            has_surveillance = np.random.choice(['是', '否'], p=[0.4, 0.6])
            parking_difficulty = '容易'

        elif parking_type == "地下停车场":
            driving_distance = np.random.randint(150, 800)
            walking_distance = np.random.randint(50, 500)
            time_to_find_parking = np.random.randint(3, 10)
            parking_space_size = np.random.randint(7, 9)
            parking_fee = round(np.random.uniform(5, 10), 2)
            near_elevator = np.random.choice(['是', '否'], p=[0.6, 0.4])
            has_surveillance = np.random.choice(['是', '否'], p=[0.8, 0.2])
            parking_difficulty = np.random.choice(['容易', '中等'])

        else:  # 住宅区或其它停车场
            driving_distance = np.random.randint(50, 400)
            walking_distance = np.random.randint(10, 300)
            time_to_find_parking = np.random.randint(1, 5)
            parking_space_size = np.random.randint(8, 10)
            parking_fee = round(np.random.uniform(3, 8), 2)
            near_elevator = np.random.choice(['是', '否'], p=[0.3, 0.7])
            has_surveillance = np.random.choice(['是', '否'], p=[0.5, 0.5])
            parking_difficulty = '容易'

        # 添加每个停车位的数据
        parking_data['ID'].append(i + 1)
        parking_data['Driving Distance (meters)'].append(driving_distance)
        parking_data['Walking Distance (meters)'].append(walking_distance)
        parking_data['Time to Find Parking (minutes)'].append(time_to_find_parking)
        parking_data['Parking Space Size (0-10)'].append(parking_space_size)
        parking_data['Parking Difficulty'].append(parking_difficulty)
        parking_data['Near Elevator'].append(near_elevator)
        parking_data['Has Surveillance'].append(has_surveillance)
        parking_data['Parking Fee (CNY/hour)'].append(parking_fee)
        parking_data['Parking Type'].append(parking_type)
        parking_data['Longitude'].append(longitude)
        parking_data['Latitude'].append(latitude)

        # 将数据转换为 DataFrame
    parking_spots_df = pd.DataFrame(parking_data)

    # 生成用户评分数据
    ratings = []

    # 定义相似性函数
    def calculate_base_rating(parking_spot):
        """
        根据停车位的属性计算基础评分。

        参数:
        parking_spot (Series): 单个停车位的属性数据

        返回:
        float: 基础评分
        """
        base_rating = 5.0
        base_rating += 0.5 if parking_spot['Parking Space Size (0-10)'] > 8 else 0
        base_rating += 0.2 if parking_spot['Near Elevator'] == '是' else 0
        base_rating += 0.2 if parking_spot['Has Surveillance'] == '是' else 0
        base_rating += 0.5 if parking_spot['Parking Difficulty'] == '容易' else 0
        base_rating -= 0.5 if parking_spot['Driving Distance (meters)'] > 900 else 0
        base_rating -= 0.5 if parking_spot['Walking Distance (meters)'] > 280 else 0
        base_rating -= 0.5 if parking_spot['Parking Fee (CNY/hour)'] > 7 else 0
        return base_rating

    # 为每个用户生成评分数据
    for user_id in range(1, num_users + 1):
        num_ratings = np.random.randint(min_ratings_per_user, max_ratings_per_user)  # 每个用户评分的停车位数量
        rated_spots = np.random.choice(parking_spots_df['ID'], num_ratings, replace=False)
        for spot_id in rated_spots:
            spot = parking_spots_df.loc[parking_spots_df['ID'] == spot_id].iloc[0]
            base_rating = calculate_base_rating(spot)
            # 使用正态分布随机扰动生成评分，并截取到1到5的范围
            rating = np.clip(base_rating + np.random.normal(0, 1.5), 1, 5)
            ratings.append([spot_id, user_id, round(rating, 1)])

    # 将用户评分数据转换为 DataFrame
    ratings_df = pd.DataFrame(ratings, columns=['停车位ID', '用户ID', '评分'])

    # 保存停车位数据和用户评分数据为CSV文件
    parking_spots_df.to_csv('../data/parking_spots_with_coords.csv', index=False)
    ratings_df.to_csv('../data/original_ratings.csv', index=False)

    return parking_spots_df, ratings_df


# 参数设置
num_users = 100
num_parking_spots = 200
min_ratings_per_user = 20
max_ratings_per_user = 40

# 使用高德地图API获取停车场数据
city = '福州'
api_key = 'da2c7cf734d2af3112d39ad74a58e284'  # 需要替换为你自己的高德地图API Key
parking_lots = fetch_parking_lots(city, api_key)

# 生成带有实际坐标的停车位数据
parking_spots, ratings_df = generate_parking_data(num_users, num_parking_spots, min_ratings_per_user,
                                                  max_ratings_per_user, parking_lots)

# 显示生成的数据
print("停车位信息:")
print(parking_spots.head())

print("\n用户评分:")
print(ratings_df.head())
