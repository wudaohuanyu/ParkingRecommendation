from py2neo import Graph, NodeMatcher
import pandas as pd


class ParkingGraphQuery:
    """
    ParkingGraphQuery类负责查询数据库中的节点信息，并提供基于用户评分的推荐功能。
    """

    def __init__(self, uri, username, password):
        """
        初始化数据库连接
        :param uri: 数据库URI
        :param username: 数据库用户名
        :param password: 数据库密码
        """
        try:
            self.graph = Graph(uri, auth=(username, password))
            self.node_matcher = NodeMatcher(self.graph)
            print("Connected to the database.")
        except Exception as e:
            raise ConnectionError(f"数据库连接失败: {str(e)}")

    def query_park_node(self, park_id):
        """
        查询停车场节点
        :param park_id: 停车场的ID
        :return: 匹配的停车场节点，如果未找到则返回消息
        """
        try:
            park_id = int(park_id)
            # 尝试从数据库中查询停车场节点
            find_node = self.node_matcher.match('ParkingSpot').where(id=park_id).first()
            if find_node:
                return find_node, None  # 返回节点对象
            else:
                return None, f"未找到ID为 {park_id} 的停车场节点。"  # 没有找到节点
        except Exception as e:
            raise Exception(f"查询停车场节点失败: {str(e)}")

    def query_user_node(self, user_id):
        """
        查询用户节点
        :param user_id: 用户的ID
        :return: 匹配的用户节点，如果未找到则返回消息
        """
        try:
            user_id = int(user_id)
            # 尝试从数据库中查询用户节点
            find_node = self.node_matcher.match('User', id=user_id).first()
            if find_node:
                return find_node, None  # 返回节点对象
            else:
                return None, f"未找到ID为 {user_id} 的用户节点。"  # 没有找到节点
        except Exception as e:
            raise Exception(f"查询用户节点失败: {str(e)}")

    def get_recommendations(self, user_id, k=10, parking_common=3, users_common=2, threshold_sim=0.9, m=5):
        """
        基于用户相似性获取停车场推荐列表

        :param user_id: 用户的ID
        :param k: 考虑的前k个最相似用户
        :param parking_common: 评估相似用户时，至少共同打分的停车场数目
        :param users_common: 被推荐的停车场至少要被几名相似用户打分
        :param threshold_sim: 用户相似度的最小阈值
        :param m: 返回的推荐停车场数量

        :return: 推荐的停车场列表（包含停车场的评分和相似用户的数量）
        """
        try:
            # 1. 清除已有的用户相似性关系
            user_id = int(user_id)
            self.graph.run(f"""
                MATCH (u1:User)-[s:SIMILARITY]-(u2:User)
                DELETE s
            """)

            # 2. 计算所有用户之间的余弦相似度，并创建相似性关系
            self.graph.run(f"""
                MATCH (u1:User {{id : {user_id}}})-[r1:RATED]-(p:ParkingSpot)-[r2:RATED]-(u2:User)
                WITH
                    u1, u2,
                    COUNT(p) AS parking_common,
                    SUM(r1.grading * r2.grading)/(SQRT(SUM(r1.grading^2)) * SQRT(SUM(r2.grading^2))) AS sim
                WHERE parking_common >= {parking_common} AND sim > {threshold_sim}
                MERGE (u1)-[s:SIMILARITY]-(u2)
                SET s.sim = sim
            """)

            # 3. 根据相似度获取推荐的停车场
            query = f"""
                MATCH (u1:User {{id : {user_id}}})-[s:SIMILARITY]-(u2:User)
                WITH u1, u2, s
                ORDER BY s.sim DESC LIMIT {k}
                MATCH (p:ParkingSpot)-[r:RATED]-(u2)
                WITH u1, u2, s, p, r
                WITH
                    p.id AS id,
                    p.driving_distance AS driving_distance,
                    p.walking_distance AS walking_distance,
                    p.found_time AS found_time,
                    p.parking_space_size AS parking_space_size,
                    p.parking_difficulty AS parking_difficulty,
                    p.near_elevator AS near_elevator,
                    p.has_surveillance AS has_surveillance,
                    p.fee AS fee,
                    p.parking_type AS parking_type,
                    p.longitude AS longitude,
                    p.latitude AS latitude,
                    SUM(r.grading * s.sim)/SUM(s.sim) AS grade,
                    COUNT(u2) AS num
                WHERE num >= {users_common}
                RETURN id, driving_distance, walking_distance, found_time, parking_space_size, parking_difficulty, near_elevator, has_surveillance, fee, parking_type, longitude, latitude, grade, num
                ORDER BY grade DESC, num DESC
                LIMIT {m}
            """

            # 执行查询并获取推荐结果
            result = self.graph.run(query)
            recommendations = []

            # 将查询结果转换为字典列表
            for record in result:
                recommendations.append({
                    "id": record["id"],
                    "driving_distance": record["driving_distance"],
                    "walking_distance": record["walking_distance"],
                    "found_time": record["found_time"],
                    "parking_space_size": record["parking_space_size"],
                    "parking_difficulty": record["parking_difficulty"],
                    "near_elevator": record["near_elevator"],
                    "has_surveillance": record["has_surveillance"],
                    "fee": record["fee"],
                    "parking_type": record["parking_type"],
                    "longitude": record["longitude"],
                    "latitude": record["latitude"],
                    "grade": record["grade"],
                    "num": record["num"],
                })

            return recommendations

        except Exception as e:
            raise Exception(f"获取推荐停车场失败: {str(e)}")
