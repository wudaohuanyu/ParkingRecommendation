from py2neo import Graph, Node, Relationship, NodeMatcher
import csv


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
            re_value = self.node_matcher.match('ParkingSpot').where(id=attrs[0]).first()
            if re_value is None:
                node = Node('ParkingSpot',
                            id=attrs[0],
                            inner_distance=attrs[1],
                            walking_distance=attrs[2],
                            found_time=attrs[3],
                            parking_space=attrs[4],
                            parking_level=attrs[5],
                            near_elevator=attrs[6],
                            monitoringfound_time=attrs[7],
                            fee=attrs[8])
                self.graph.create(node)
                return node
            return None
        except Exception as e:
            raise Exception(f"Failed to create parking node: {str(e)}")

    def create_user_node(self, attrs):
        try:
            re_value = self.node_matcher.match('User').where(id=attrs[1]).first()
            if re_value is None:
                node = Node('User', id=attrs[1])
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
            relation = Relationship(user_value, attrs[2], park_value)
            self.graph.create(relation)
            return True, "Rating relation created successfully."
        except Exception as e:
            raise Exception(f"Failed to create rating relation: {str(e)}")

    def match_park_node(self, attrs):
        try:
            return self.node_matcher.match('ParkingSpot').where(id=attrs[0]).first()
        except Exception as e:
            raise Exception(f"Failed to match parking spot: {str(e)}")

    def match_user_node(self, attrs):
        try:
            return self.node_matcher.match('User').where(id=attrs[1]).first()
        except Exception as e:
            raise Exception(f"Failed to match user: {str(e)}")

    def query_park_node(self, park_id):
        try:
            find_node = self.node_matcher.match('ParkingSpot').where(id=park_id).first()
            if find_node:
                return find_node
            else:
                return None, f"ParkingSpot with id {park_id} not found."
        except Exception as e:
            raise Exception(f"Failed to query parking spot: {str(e)}")

    def query_user_node(self, user_id):
        try:
            find_node = self.node_matcher.match('User').where(id=user_id).first()
            if find_node:
                return find_node
            else:
                return None, f"User with id {user_id} not found."
        except Exception as e:
            raise Exception(f"Failed to query user: {str(e)}")

    def update_user_node(self, user_id, update_data):
        try:
            # 查找用户节点
            user_node = self.query_user_node(user_id)
            if user_node is None:
                return False, f"User with id {user_id} not found."

            # 更新用户节点
            for key, value in update_data.items():
                user_node[key] = value

            # 推送更新
            self.graph.push(user_node)
            return True, "User updated successfully."
        except Exception as e:
            raise Exception(f"Failed to update user node: {str(e)}")


# Initialization (this part would typically be done in your main application)
uri = "http://localhost:7474"
username = "neo4j"
password = "cwy123456"

# Create an instance of ParkingGraph
parking_graph = ParkingGraph(uri, username, password)

# Example usage (these would be function calls from your application logic)
try:
    # Load parking spot data from CSV
    parking_node_data = parking_graph.read_csv_file("data/parking_spots.csv")

    # Load user data from CSV
    user_node_data = parking_graph.read_csv_file("data/original_ratings.csv")

    # Create nodes
    for i in range(1, len(parking_node_data)):
        parking_graph.create_parking_node(parking_node_data[i])

    for j in range(1, len(user_node_data)):
        parking_graph.create_user_node(user_node_data[j])

    # Create relationships
    for m in range(1, len(user_node_data)):
        result, message = parking_graph.create_rating_relation(user_node_data[m])
        print(result, message)

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

except Exception as e:
    print(str(e))
