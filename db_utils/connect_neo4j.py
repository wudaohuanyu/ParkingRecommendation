import csv
import os
import dotenv
from neo4j import GraphDatabase
from tqdm import tqdm  # 导入 tqdm 库

# 加载环境变量
load_status = dotenv.load_dotenv("../Neo4j-fe89fc25-Created-2024-09-29.txt")
if not load_status:
    raise RuntimeError('Environment variables not loaded.')

# 获取 URI 和 AUTH
URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

# 创建驱动程序实例
driver = GraphDatabase.driver(URI, auth=AUTH)


def load_parking_spots(file_path):
    """
    从 parking_spots.csv 文件中加载停车位数据。
    返回停车位记录的列表，每条记录为字典格式。
    """
    parking_spots = []
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # 转换每条记录为字典
            parking_spots.append({
                'id': int(row['ID']),
                'drive_distance': float(row['场内行驶距离(米)']),
                'walk_distance': float(row['步行距离(米)']),
                'search_time': float(row['寻找到停泊位所花费的时间(分钟)']),
                'space_size': int(row['泊位空间大小(0-10)']),
                'difficulty': row['停车难易度'],
                'near_elevator': row['是否靠近电梯'] == '是',
                'has_surveillance': row['是否有监控'] == '是',
                'cost_per_hour': float(row['停车费用(元/小时)'])
            })
    return parking_spots


def load_ratings(file_path):
    """
    从 original_ratings.csv 文件中加载评分数据。
    返回评分记录的列表，每条记录为字典格式。
    """
    ratings = []
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            ratings.append({
                'parking_spot_id': int(row['停车位ID']),
                'user_id': int(row['用户ID']),
                'rating': float(row['评分'])
            })
    return ratings


def insert_parking_spots(tx, parking_spots):
    """
    将停车位数据插入到数据库中。
    :param tx: 数据库事务
    :param parking_spots: 停车位的数据列表
    """
    for spot in tqdm(parking_spots, desc="Inserting Parking Spots", unit="spot"):  # 使用 tqdm 显示进度条
        tx.run("""
            MERGE (p:ParkingSpot {id: $id})
            SET p.drive_distance = $drive_distance,
                p.walk_distance = $walk_distance,
                p.search_time = $search_time,
                p.space_size = $space_size,
                p.difficulty = $difficulty,
                p.near_elevator = $near_elevator,
                p.has_surveillance = $has_surveillance,
                p.cost_per_hour = $cost_per_hour
        """, **spot)


def insert_ratings(tx, ratings):
    """
    将评分数据插入到数据库中，创建用户与停车位之间的 RATED 关系。
    :param tx: 数据库事务
    :param ratings: 用户对停车位评分的数据列表
    """
    for rating in tqdm(ratings, desc="Inserting User Ratings", unit="rating"):  # 使用 tqdm 显示进度条
        tx.run("""
            MERGE (u:User {id: $user_id})
            MERGE (p:ParkingSpot {id: $parking_spot_id})
            MERGE (u)-[r:RATED]->(p)
            SET r.grading = $rating
        """, **rating)


def insert_data_into_neo4j(parking_spots_file, ratings_file):
    """
    从文件中读取数据并插入到 Neo4j 数据库。
    :param parking_spots_file: 停车位的 CSV 文件路径
    :param ratings_file: 用户对停车位的评分 CSV 文件路径
    """
    # 读取数据
    parking_spots = load_parking_spots(parking_spots_file)
    ratings = load_ratings(ratings_file)

    try:
        with driver.session() as session:
            # 验证连接是否成功
            driver.verify_connectivity()
            print("Connection established.")

            # 插入停车场数据
            session.execute_write(insert_parking_spots, parking_spots)
            print(f"Inserted {len(parking_spots)} parking spots.")

            # 插入用户评分关系
            session.execute_write(insert_ratings, ratings)
            print(f"Inserted {len(ratings)} ratings.")

    except Exception as e:
        print(f"Failed to insert data into Neo4j: {e}")
    finally:
        # 关闭驱动程序
        driver.close()


if __name__ == "__main__":
    # CSV 文件路径
    parking_spots_file = "../data/parking_spots.csv"
    ratings_file = "../data/original_ratings.csv"

    # 插入数据到 Neo4j
    insert_data_into_neo4j(parking_spots_file, ratings_file)
