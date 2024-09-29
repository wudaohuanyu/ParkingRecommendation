import dotenv
import os
from neo4j import GraphDatabase

load_status = dotenv.load_dotenv("Neo4j-fe89fc25-Created-2024-09-29.txt")
if load_status is False:
    raise RuntimeError('Environment variables not loaded.')

URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

# 创建驱动程序实例
driver = GraphDatabase.driver(URI, auth=AUTH)


def get_all_nodes(tx):
    # 查询所有节点及其属性
    result = tx.run("MATCH (n) RETURN n LIMIT 100")
    return [record["n"] for record in result]


try:
    with driver.session() as session:
        # 验证连接是否成功
        driver.verify_connectivity()
        print("Connection established.")

        # 查询所有节点
        nodes = session.execute_read(get_all_nodes)
        for node in nodes:
            print(node)
except Exception as e:
    print(f"Failed to connect to Neo4j: {e}")
finally:
    # 关闭驱动程序
    driver.close()
