from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta

# Token settings
SECRET_KEY = "your_secret_key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing settings
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

app = FastAPI()

# Fake databases for the example
fake_users_db = {}
fake_parking_db = {}


class User(BaseModel):
    username: str
    email: str
    hashed_password: str


class UserInDB(User):
    hashed_password: str


class UserCreate(BaseModel):
    username: str
    password: str
    email: str


class UserPreferences(BaseModel):
    location_preference: str
    time_preference: str


class ParkingSpot(BaseModel):
    parking_id: str
    location: str
    availability: bool
    price: float


class ParkingRecommendationRequest(BaseModel):
    user_id: str
    location: str
    time: str


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        user = fake_users_db.get(username)
        if user is None:
            raise credentials_exception
        return user
    except JWTError:
        raise credentials_exception


@app.post("/register")
async def register(user: UserCreate):
    if user.username in fake_users_db:
        raise HTTPException(status_code=400, detail="Username already registered")
    hashed_password = get_password_hash(user.password)
    fake_users_db[user.username] = UserInDB(**user.dict(), hashed_password=hashed_password)
    return {"msg": "User registered successfully"}


@app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = fake_users_db.get(form_data.username)
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": user.username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/user/{user_id}")
async def get_user(user_id: str, current_user: User = Depends(get_current_user)):
    user = fake_users_db.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.put("/user/{user_id}")
async def update_user_preferences(user_id: str, preferences: UserPreferences,
                                  current_user: User = Depends(get_current_user)):
    user = fake_users_db.get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.preferences = preferences
    return {"msg": "User preferences updated successfully"}


@app.get("/parking/{parking_id}")
async def get_parking(parking_id: str):
    parking_spot = fake_parking_db.get(parking_id)
    if not parking_spot:
        raise HTTPException(status_code=404, detail="Parking spot not found")
    return parking_spot


@app.put("/parking/{parking_id}")
async def update_parking(parking_id: str, parking_spot: ParkingSpot):
    if parking_id not in fake_parking_db:
        raise HTTPException(status_code=404, detail="Parking spot not found")
    fake_parking_db[parking_id] = parking_spot
    return {"msg": "Parking spot information updated successfully"}


@app.post("/recommendations")
async def get_recommendations(request: ParkingRecommendationRequest):
    # This is where you would add your recommendation logic
    # For simplicity, we're just returning all parking spots
    recommendations = [spot for spot in fake_parking_db.values()]
    if not recommendations:
        raise HTTPException(status_code=404, detail="No recommendations found")
    return recommendations


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=5000)
