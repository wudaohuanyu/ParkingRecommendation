from fastapi import FastAPI, HTTPException, Depends, Request
from pydantic import BaseModel
from typing import List, Optional
# from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
# from jose import JWTError, jwt
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
import os
import dotenv

# 导入自定义模块
from db_utils.parking_graph_manager import ParkingGraphManager
from db_utils.parking_graph_query import ParkingGraphQuery

# # 令牌配置
# SECRET_KEY = "your_secret_key"
# ALGORITHM = "HS256"
# ACCESS_TOKEN_EXPIRE_MINUTES = 30
#
# # 密码哈希配置
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

app = FastAPI()

# 启用 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许的域名，可以填写具体的前端域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 初始化 ParkingGraphManager 和 ParkingGraphQuery
load_status = dotenv.load_dotenv("Neo4j-fe89fc25-Created-2024-09-29.txt")
if load_status is False:
    raise RuntimeError('Environment variables not loaded.')

URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

parking_graph_manager = ParkingGraphManager(URI, AUTH[0], AUTH[1])
parking_graph_query = ParkingGraphQuery(URI, AUTH[0], AUTH[1])

# Pydantic 模型定义
class UserCreate(BaseModel):
    username: str
    password: str


class UserPreferences(BaseModel):
    preferred_parking_types: List[str]  # 用户偏好停车场类型列表，例如 ['商场', '地面停车场']
    max_parking_fee: Optional[float] = None  # 用户能够接受的最大停车费用
    preferred_parking_difficulty: Optional[str] = None  # 用户偏好的停车难度（‘容易’、‘中等’、‘困难’）
    max_walking_distance: Optional[int] = None  # 用户能够接受的最大步行距离（米）
    max_driving_distance: Optional[int] = None  # 用户能够接受的最大行车距离（米）


class ParkingSpot(BaseModel):
    parking_id: int
    inner_distance: int
    walking_distance: int
    found_time: int
    parking_space: int
    parking_level: str
    near_elevator: bool
    monitoringfound_time: str
    fee: float


class ParkingRecommendationRequest(BaseModel):
    user_id: int
    location: str
    time: str


# # 密码验证函数
# def verify_password(plain_password, hashed_password):
#     return pwd_context.verify(plain_password, hashed_password)
#
#
# # 获取密码哈希值函数
# def get_password_hash(password):
#     return pwd_context.hash(password)
#
#
# # 创建访问令牌函数
# def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
#     to_encode = data.copy()
#     if expires_delta:
#         expire = datetime.utcnow() + expires_delta
#     else:
#         expire = datetime.utcnow() + timedelta(minutes=15)
#     to_encode.update({"exp": expire})
#     encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
#     return encoded_jwt
#
#
# # 获取当前用户函数
# async def get_current_user(token: str = Depends(oauth2_scheme)):
#     credentials_exception = HTTPException(
#         status_code=401,
#         detail="无法验证凭据",
#         headers={"WWW-Authenticate": "Bearer"},
#     )
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         username: str = payload.get("sub")
#         if username is None:
#             raise credentials_exception
#         user_node, message = parking_graph_query.query_user_node(username)
#         if user_node is None:
#             raise credentials_exception
#         return user_node
#     except JWTError:
#         raise credentials_exception


# # 注册新用户
# @app.post("/register")
# async def register(user: UserCreate):
#     existing_user, message = parking_graph_query.query_user_node(user.username)
#     if existing_user:
#         raise HTTPException(status_code=400, detail="用户名已被注册")
#     hashed_password = get_password_hash(user.password)
#     user_data = {
#         "id": user.username,
#         "hashed_password": hashed_password
#     }
#     parking_graph_manager.create_user_node(user_data)
#     return {"msg": "用户注册成功"}
#
#
# # 用户登录
# @app.post("/login")
# async def login(form_data: OAuth2PasswordRequestForm = Depends()):
#     user_node, message = parking_graph_query.query_user_node(form_data.username)
#     if not user_node or not verify_password(form_data.password, user_node["hashed_password"]):
#         raise HTTPException(status_code=401, detail="无效的凭据")
#     access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
#     access_token = create_access_token(data={"sub": user_node["id"]}, expires_delta=access_token_expires)
#     return {"access_token": access_token, "token_type": "bearer"}


# 获取用户偏好信息
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def read_index(request: Request):
    """
    处理根路径请求，返回 index.html 作为响应
    """
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/user/{user_id}")
async def get_user_preferences(user_id: int):
    user_node, message = parking_graph_query.query_user_node(user_id)
    if not user_node:
        raise HTTPException(status_code=404, detail="未找到用户")

    # 返回用户的偏好信息，增加新的字段
    return {
        "preferred_parking_types": user_node.get("preferred_parking_types", []),
        "max_parking_fee": user_node.get("max_parking_fee", None),
        "preferred_parking_difficulty": user_node.get("preferred_parking_difficulty", "中等"),
        "max_walking_distance": user_node.get("max_walking_distance", None),
        "max_driving_distance": user_node.get("max_driving_distance", None),
    }


# 更新用户偏好信息
@app.put("/user/{user_id}")
async def update_user_preferences(user_id: int, preferences: UserPreferences):
    update_data = preferences.dict()

    # 更新用户节点
    result, message = parking_graph_manager.update_user_node(user_id, update_data)
    if not result:
        raise HTTPException(status_code=404, detail=message)

    return {"msg": "用户偏好更新成功"}


# 获取停车位信息
@app.get("/parking/{parking_id}")
async def get_parking(parking_id: int):
    parking_node, message = parking_graph_query.query_park_node(parking_id)
    if not parking_node:
        raise HTTPException(status_code=404, detail=message)
    return dict(parking_node)


# # 更新停车位信息
# @app.put("/parking/{parking_id}")
# async def update_parking(parking_id: str, parking_spot: ParkingSpot):
#     update_data = parking_spot.dict()
#     parking_node, message = parking_graph.query_park_node(parking_id)
#     if not parking_node:
#         raise HTTPException(status_code=404, detail=message)
#     parking_graph.update_parking_node(parking_id, update_data)
#     return {"msg": "停车位信息更新成功"}
#
#
# # 获取停车推荐
# @app.post("/recommendations")
# async def get_recommendations(request: ParkingRecommendationRequest):
#     # 这里是添加推荐逻辑的地方
#     # 为简化起见，我们只是返回所有停车位信息
#     recommendations = parking_graph.get_all_parking_spots()
#     if not recommendations:
#         raise HTTPException(status_code=404, detail="未找到推荐")
#     return recommendations

# 获取停车推荐
@app.get("/recommendations/{user_id}")
async def get_recommendations(user_id: str):
    recommendations = parking_graph_query.get_recommendations(user_id)
    if not recommendations:
        raise HTTPException(status_code=404, detail="未找到推荐")
    return recommendations


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=5000)
