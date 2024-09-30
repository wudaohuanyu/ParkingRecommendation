import pandas as pd
import numpy as np


def generate_parking_data(num_users, num_parking_spots, min_ratings_per_user, max_ratings_per_user, seed=42):
    """
    生成停车位数据和用户评分数据，并保存为CSV文件。

    参数:
    num_users (int): 用户数量
    num_parking_spots (int): 停车位数量
    min_ratings_per_user (int): 每个用户评分的最少停车位数量
    max_ratings_per_user (int): 每个用户评分的最多停车位数量
    seed (int): 随机种子，默认值为42

    返回:
    parking_spots (DataFrame): 停车位信息数据DataFrame
    ratings_df (DataFrame): 用户评分数据DataFrame
    """
    # 设置随机种子
    np.random.seed(seed)

    # 生成停车位数据
    parking_spots = pd.DataFrame({
        'ID': range(1, num_parking_spots + 1),
        '场内行驶距离(米)': np.random.randint(70, 1600, num_parking_spots),
        '步行距离(米)': np.random.randint(10, 600, num_parking_spots),
        '寻找到停泊位所花费的时间(分钟)': np.random.randint(1, 15, num_parking_spots),
        '泊位空间大小(0-10)': np.random.randint(1, 11, num_parking_spots),
        '停车难易度': np.random.choice(['容易', '中等', '困难'], num_parking_spots),
        '是否靠近电梯': np.random.choice(['是', '否'], num_parking_spots, p=[0.25, 0.75]),
        '是否有监控': np.random.choice(['是', '否'], num_parking_spots, p=[0.45, 0.55]),
        '停车费用(元/小时)': np.random.uniform(3, 10, num_parking_spots).round(2)
    })

    # 生成用户评分数据
    ratings = []

    # 定义相似性函数
    def calculate_base_rating(parking_spot):
        base_rating = 5
        if parking_spot['泊位空间大小(0-10)'] > 8:
            base_rating += 0.5
        if parking_spot['是否靠近电梯'] == '是':
            base_rating += 0.2
        if parking_spot['是否有监控'] == '是':
            base_rating += 0.2
        if parking_spot['停车难易度'] == '容易':
            base_rating += 0.5
        if parking_spot['场内行驶距离(米)'] > 900:
            base_rating -= 0.5
        if parking_spot['步行距离(米)'] > 280:
            base_rating -= 0.5
        if parking_spot['停车费用(元/小时)'] > 7:
            base_rating -= 0.5
        return base_rating

    for user_id in range(1, num_users + 1):
        num_ratings = np.random.randint(min_ratings_per_user, max_ratings_per_user)  # 每个用户评分的停车位数量
        rated_spots = np.random.choice(parking_spots['ID'], num_ratings, replace=False)
        for spot_id in rated_spots:
            spot = parking_spots.loc[parking_spots['ID'] == spot_id].iloc[0]
            base_rating = calculate_base_rating(spot)
            # 添加更大范围的随机扰动
            rating = np.clip(base_rating + np.random.normal(0, 1.5), 1, 5)
            ratings.append([spot_id, user_id, round(rating, 1)])

    ratings_df = pd.DataFrame(ratings, columns=['停车位ID', '用户ID', '评分'])

    # 保存为CSV文件
    parking_spots.to_csv('parking_spots.csv', index=False)
    ratings_df.to_csv('original_ratings.csv', index=False)

    return parking_spots, ratings_df


# 参数设置
num_users = 100
num_parking_spots = 200
min_ratings_per_user = 20
max_ratings_per_user = 40

parking_spots, ratings_df = generate_parking_data(num_users, num_parking_spots, min_ratings_per_user,
                                                  max_ratings_per_user)

# 显示生成的数据
print("停车位信息:")
print(parking_spots.head())

print("\n用户评分:")
print(ratings_df.head())
