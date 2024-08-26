from py2neo import Graph, Node, Relationship, NodeMatcher
import csv
import pandas as pd


class ParkingGraph:
    def __init__(self, uri, username, password):
        try:
            self.graph = Graph(uri, auth=(username, password))
            self.node_matcher = NodeMatcher(self.graph)
            print("Connected to the database.")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to the database: {str(e)}")

    def read_csv_file(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                data = list(reader)
            return data
        except FileNotFoundError:
            raise FileNotFoundError(f"The file {file_path} was not found.")
        except Exception as e:
            raise Exception(f"An error occurred while reading the file: {str(e)}")

    def create_parking_node(self, attrs):
        try:
            re_value = self.node_matcher.match('ParkingSpot').where(id=int(attrs[0])).first()
            if re_value is None:
                node = Node('ParkingSpot',
                            id=int(attrs[0]),
                            inner_distance=int(attrs[1]),
                            walking_distance=int(attrs[2]),
                            found_time=int(attrs[3]),
                            parking_space=int(attrs[4]),
                            parking_level=attrs[5],
                            near_elevator=attrs[6],
                            monitoringfound_time=attrs[7],
                            fee=float(attrs[8]))
                self.graph.create(node)
                return node
            return None
        except Exception as e:
            raise Exception(f"Failed to create parking node: {str(e)}")

    def create_user_node(self, attrs):
        try:
            re_value = self.node_matcher.match('User').where(id=int(attrs[1])).first()
            if re_value is None:
                node = Node('User', id=int(attrs[1]))
                self.graph.create(node)
                return node
            return None
        except Exception as e:
            raise Exception(f"Failed to create user node: {str(e)}")

    def create_rating_relation(self, attrs):
        try:
            park_value = self.match_park_node(attrs)
            user_value = self.match_user_node(attrs)
            if park_value is None or user_value is None:
                return False, "Either ParkingSpot or User node not found."
            relation = Relationship(user_value, 'RATED', park_value, grading=float(attrs[2]))
            self.graph.create(relation)
            return True, "Rating relation created successfully."
        except Exception as e:
            raise Exception(f"Failed to create rating relation: {str(e)}")

    def match_park_node(self, attrs):
        try:
            return self.node_matcher.match('ParkingSpot').where(id=int(attrs[0])).first()
        except Exception as e:
            raise Exception(f"Failed to match parking spot: {str(e)}")

    def match_user_node(self, attrs):
        try:
            return self.node_matcher.match('User').where(id=int(attrs[1])).first()
        except Exception as e:
            raise Exception(f"Failed to match user: {str(e)}")

    def query_park_node(self, park_id):
        try:
            park_id = int(park_id)
            find_node = self.node_matcher.match('ParkingSpot').where(id=park_id).first()
            if find_node:
                return find_node, None
            else:
                return None, f"ParkingSpot with id {park_id} not found."
        except Exception as e:
            raise Exception(f"Failed to query parking spot: {str(e)}")

    def query_user_node(self, user_id):
        try:
            user_id = int(user_id)  # 确保user_id被转换为整数
            # print(f"Querying user node for User ID: {user_id}")
            find_node = self.node_matcher.match('User', id=user_id).first()
            if find_node:
                # print(f"User node found for User ID: {user_id}")
                return find_node, None
            else:
                print(f"User node not found for User ID: {user_id}")
                return None, f"User with id {user_id} not found."
        except Exception as e:
            raise Exception(f"Failed to query user: {str(e)}")

    def update_user_node(self, user_id, update_data):
        try:
            # 查找用户节点，解包返回的元组
            user_node, error_message = self.query_user_node(user_id)
            if not user_node:
                return None, error_message

            # 更新用户节点
            for key, value in update_data.items():
                user_node[key] = value

            # 推送更新
            self.graph.push(user_node)
            return True, "User updated successfully."
        except Exception as e:
            raise Exception(f"Failed to update user node: {str(e)}")

    def get_recommendations(self, user_id, k=10, parking_common=3, users_common=2, threshold_sim=0.9, m=5):
        # 1. 清除已有的相似度关系
        user_id = int(user_id)
        self.graph.run(f"""
            MATCH (u1:User)-[s:SIMILARITY]-(u2:User)
            DELETE s
        """)

        # 2. 计算用户之间的余弦相似度
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

        # 3. 获取推荐的停车场
        query = f"""
            MATCH (u1:User {{id : {user_id}}})-[s:SIMILARITY]-(u2:User)
            WITH u1, u2, s
            ORDER BY s.sim DESC LIMIT {k}
            MATCH (p:ParkingSpot)-[r:RATED]-(u2)
            WITH u1, u2, s, p, r //, COLLECT(DISTINCT g.genre) AS gen
            WITH
                p.id AS id,
                p.inner_distance AS inner_distance,
                p.walking_distance AS walking_distance,
                p.found_time AS found_time,
                p.parking_space AS parking_space,
                p.parking_level AS parking_level,
                p.near_elevator AS near_elevator,
                p.monitoringfound_time AS monitoringfound_time,
                p.fee AS fee,
                SUM(r.grading * s.sim)/SUM(s.sim) AS grade,
                COUNT(u2) AS num
            WHERE num >= {users_common}
            RETURN id, inner_distance, walking_distance, found_time, parking_space, parking_level, near_elevator, monitoringfound_time, fee, grade, num
            ORDER BY grade DESC, num DESC
            LIMIT {m}
        """

        # query = f"""
        #             MATCH (u1:User {{id : {user_id}}})-[s:SIMILARITY]-(u2:User)
        #             WITH u1, u2, s
        #             ORDER BY s.sim DESC LIMIT {k}
        #             MATCH (p:ParkingSpot)-[r:RATED]-(u2)
        #             // OPTIONAL MATCH (g:Genre)--(p)
        #             WITH u1, u2, s, p, r //, COLLECT(DISTINCT g.genre) AS gen
        #             // WHERE NOT((p)-[:RATED]-(u1))
        #             WITH
        #                 p.id AS id,
        #                 p.inner_distance AS inner_distance,
        #                 p.walking_distance AS walking_distance,
        #                 p.found_time AS found_time,
        #                 p.parking_space AS parking_space,
        #                 p.parking_level AS parking_level,
        #                 p.near_elevator AS near_elevator,
        #                 p.monitoringfound_time AS monitoringfound_time,
        #                 p.fee AS fee,
        #                 SUM(r.grading * s.sim)/SUM(s.sim) AS grade,
        #                 COUNT(u2) AS num,
        #                 // gen
        #             WHERE num >= {users_common}
        #             RETURN id, inner_distance, walking_distance, found_time, parking_space, parking_level, near_elevator, monitoringfound_time, fee, grade, num, gen
        #             ORDER BY grade DESC, num DESC
        #             LIMIT {m}
        #         """

        result = self.graph.run(query)
        recommendations = []
        for record in result:
            recommendations.append({
                "id": record["id"],
                "inner_distance": record["inner_distance"],
                "walking_distance": record["walking_distance"],
                "found_time": record["found_time"],
                "parking_space": record["parking_space"],
                "parking_level": record["parking_level"],
                "near_elevator": record["near_elevator"],
                "monitoringfound_time": record["monitoringfound_time"],
                "fee": record["fee"],
                "grade": record["grade"],
                "num": record["num"],
                # "genres": record["gen"]
            })

        return recommendations


if __name__ == '__main__':

    # Initialization (this part would typically be done in your main application)
    uri = "neo4j://localhost:7687"
    username = "neo4j"
    password = "meiyuan0369"

    # Create an instance of ParkingGraph
    parking_graph = ParkingGraph(uri, username, password)

    # Example usage (these would be function calls from your application logic)
    try:
        # Load parking spot data from CSV
        parking_node_data = parking_graph.read_csv_file("../data/parking_spots.csv")

        # Load user data from CSV
        user_node_data = parking_graph.read_csv_file("../data/original_ratings.csv")

        # Create nodes
        for i in range(1, len(parking_node_data)):
            parking_graph.create_parking_node(parking_node_data[i])

        for j in range(1, len(user_node_data)):
            parking_graph.create_user_node(user_node_data[j])

        # Create relationships
        for m in range(1, len(user_node_data)):
            result, message = parking_graph.create_rating_relation(user_node_data[m])
            # print(result, message)

        print("节点、关系加载完成.")

        # Query and update
        park_node, park_message = parking_graph.query_park_node("20")
        print(park_node, park_message)

        # Dummy data for update example (this should come from your application logic)
        update_data_example = {
            "昵称": "新昵称",
            "性别": "男",
            "可接受的场内行驶距离": "100米",
            "可接受的步行距离": "50米",
            "可接受的停泊时间": "10分钟",
            "可接受的停泊空间大小": "8",
            "可接受的停车难易度": "中等",
            "泊车位靠近电梯": "是",
            "泊车位有监控管理": "否",
            "可接受的停车费用": "5元/小时"
        }

        # Example function call for updating user node
        # result, message = parking_graph.update_user_node("20", update_data_example)
        # print(result, message)

        user_node, user_message = parking_graph.query_user_node("20")
        print(user_node, user_message)

        # 测试get_recommendations
        recommendations = parking_graph.get_recommendations("20")
        df = pd.DataFrame(recommendations)
        print(df)

    except Exception as e:
        print(str(e))
