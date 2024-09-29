from py2neo import Graph
import os

class ParkingDataConverter:
    """
    ParkingDataConverter类用于从Neo4j数据库中读取用户的停车场评分数据，并根据给定的评分阈值将其转换为训练数据集格式。
    """

    def __init__(self, uri, username, password, rating_threshold=3.5):
        """
        初始化ParkingDataConverter类，连接Neo4j数据库，并设置评分阈值。

        :param uri: Neo4j数据库URI
        :param username: 数据库用户名
        :param password: 数据库密码
        :param rating_threshold: 用于定义正向互动的评分阈值，默认为3.5
        """
        try:
            self.graph = Graph(uri, auth=(username, password))
            self.rating_threshold = rating_threshold
            print(f"成功连接至Neo4j数据库: {uri}")
        except Exception as e:
            raise ConnectionError(f"数据库连接失败: {str(e)}")

    def fetch_user_interactions(self):
        """
        从Neo4j数据库中查询用户-停车场的评分互动关系。

        :return: 字典，键为用户ID，值为用户正向互动的停车场ID列表
        """
        try:
            user_interactions = {}

            # 定义在Neo4j中的Cypher查询 
            query = """
            MATCH (u:User)-[r:RATED]->(p:ParkingSpot)
            RETURN u.id AS user_id, p.id AS parking_spot_id, r.grading AS rating
            """

            # 执行查询并获取结果
            result = self.graph.run(query)

            # 遍历查询结果，构建用户与正向评分的停车场ID列表
            for record in result:
                user_id = record["user_id"]
                parking_spot_id = record["parking_spot_id"]
                rating = record["rating"]

                # 如果评分大于等于阈值，记录此互动
                if rating >= self.rating_threshold:
                    if user_id not in user_interactions:
                        user_interactions[user_id] = []
                    user_interactions[user_id].append(parking_spot_id)

            return user_interactions

        except Exception as e:
            raise Exception(f"查询用户-停车场互动数据失败: {str(e)}")

    def save_to_file(self, user_interactions, output_file):
        """
        将用户与停车场的正向互动数据保存为训练格式的文件。

        :param user_interactions: 字典，键为用户ID，值为用户正向互动的停车场ID列表
        :param output_file: 保存的文件路径
        """
        try:
            # 确保输出目录存在
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # 将结果写入文件
            with open(output_file, 'w') as outfile:
                for user_id, parking_spots in user_interactions.items():
                    parking_spots_str = ','.join(map(str, parking_spots))
                    outfile.write(f"{user_id}\t{parking_spots_str}\n")

            print(f"转换完成，数据已写入: {output_file}")

        except Exception as e:
            raise Exception(f"保存文件时出错: {str(e)}")

    def convert_and_save(self, output_file):
        """
        从Neo4j数据库中读取评分数据并保存为训练格式的文件。

        :param output_file: 保存的文件路径
        """
        print("正在从数据库获取用户互动数据...")
        user_interactions = self.fetch_user_interactions()

        print("正在保存数据到文件...")
        self.save_to_file(user_interactions, output_file)


if __name__ == "__main__":
    # 数据库连接配置
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "your_password"
    
    # 输出文件路径
    output_file = '../data/train.txt'
    
    # 初始化并执行转换
    converter = ParkingDataConverter(uri, username, password, rating_threshold=3.5)
    converter.convert_and_save(output_file)
