from py2neo import Graph, Node, Relationship, NodeMatcher
import csv

"""
负责与停车场和用户节点的创建、关系的创建和更新相关的功能
"""


class ParkingGraphManager:
    """
    ParkingGraphManager类负责管理停车场和用户节点的创建、更新以及关系的创建。
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
            raise ConnectionError(f"Failed to connect to the database: {str(e)}")

    def read_csv_file(self, file_path):
        """
        读取CSV文件并将内容存入列表
        :param file_path: CSV文件路径
        :return: CSV内容（列表形式）
        """
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
        """
        创建停车场节点，如果节点不存在则创建
        :param attrs: 节点属性列表
        :return: 创建的节点或者None
        """
        try:
            # 检查停车场节点是否存在
            re_value = self.node_matcher.match('ParkingSpot').where(id=int(attrs[0])).first()
            if re_value is None:
                # 创建停车场节点，映射新的属性
                node = Node('ParkingSpot',
                            id=int(attrs[0]),  # ID
                            driving_distance=int(attrs[1]),  # Driving Distance (meters)
                            walking_distance=int(attrs[2]),  # Walking Distance (meters)
                            found_time=int(attrs[3]),  # Time to Find Parking (minutes)
                            parking_space_size=int(attrs[4]),  # Parking Space Size (0-10)
                            parking_difficulty=attrs[5],  # Parking Difficulty
                            near_elevator=attrs[6],  # Near Elevator
                            has_surveillance=attrs[7],  # Has Surveillance
                            fee=float(attrs[8]),  # Parking Fee (CNY/hour)
                            parking_type=attrs[9],  # Parking Type
                            longitude=float(attrs[10]),  # Longitude
                            latitude=float(attrs[11]))  # Latitude
                self.graph.create(node)
                return node
            return None
        except Exception as e:
            raise Exception(f"Failed to create parking node: {str(e)}")

    def create_user_node(self, attrs):
        """
        创建用户节点
        :param attrs: 用户节点属性列表
        :return: 创建的用户节点或者None
        """
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
        """
        为用户和停车场创建评分关系
        :param attrs: 关系属性列表
        :return: 创建结果和对应的消息
        """
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
        """
        匹配停车场节点
        :param attrs: 节点属性
        :return: 匹配的停车场节点
        """
        try:
            return self.node_matcher.match('ParkingSpot').where(id=int(attrs[0])).first()
        except Exception as e:
            raise Exception(f"Failed to match parking spot: {str(e)}")

    def match_user_node(self, attrs):
        """
        匹配用户节点
        :param attrs: 节点属性
        :return: 匹配的用户节点
        """
        try:
            return self.node_matcher.match('User').where(id=int(attrs[1])).first()
        except Exception as e:
            raise Exception(f"Failed to match user: {str(e)}")

    def update_user_node(self, user_id, update_data):
        """
        更新用户节点
        :param user_id: 用户ID
        :param update_data: 需要更新的数据（字典形式）
        :return: 更新结果和对应的消息
        """
        try:

            user_node, error_message = self.query_user_node(user_id)
            if not user_node:
                return None, error_message

            for key, value in update_data.items():
                user_node[key] = value

            self.graph.push(user_node)
            return True, "User updated successfully."
        except Exception as e:
            raise Exception(f"Failed to update user node: {str(e)}")

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
