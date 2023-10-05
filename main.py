import requests
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from datetime import datetime, time

SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class Weather(Base):
    __tablename__ = "weather"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    city = Column(String)
    main_weather_now = Column(String)
    temperature = Column(String)
    wind = Column(String)
    date_now = Column(String)
    time = Column(String)


Base.metadata.create_all(bind=engine)

app = FastAPI()


class InputData(BaseModel):
    cityname: str


@app.post("/cityweather")
def get_weather(data: InputData):
    inserted_dt = datetime.now().strftime("%Y-%m-%d")
    inserted_time = datetime.now().strftime("%H")
    with engine.connect() as conn:
        res = conn.execute(
            text(
                f"SELECT city, main_weather_now, temperature, wind, date_now "
                f"FROM weather WHERE city = '{data.cityname}' AND date_now = '{inserted_dt}'AND time = '{inserted_time}'"
            )
        )
        if res.fetchone():
            data_from_table = res.fetchone()
            result = {
                "city": data_from_table[0],
                "main_weather_now": data_from_table[1],
                "temperature": data_from_table[2],
                "wind": data_from_table[3],
                "date_now": data_from_table[4],
            }
            return result
        else:
            url = (
                "https://api.openweathermap.org/data/2.5/weather?q="
                + data.cityname
                + "&appid=d73ace91e527715f74ba23297e6303a2"
            )
            r = requests.get(url)
            d = r.json()
            main = d["main"]
            temp = round(int(main["temp"]) - 273.15)

            main_w = d["wind"]
            win = main_w["deg"]
            if win in range(0, 24) or win in range(337, 361):
                wind = "north"
            elif win in range(24, 69):
                wind = "northeast"
            elif win in range(69, 114):
                wind = "east"
            elif win in range(114, 159):
                wind = "southeast"
            elif win in range(159, 204):
                wind = "south"
            elif win in range(204, 249):
                wind = "southwest"
            elif win in range(249, 294):
                wind = "west"
            else:
                wind = "northwest"


            weather = d["weather"]
            wthr = weather[0]
            main_weather_now = wthr["description"]

            conn.execute(
                text(
                    f"INSERT INTO weather (city, main_weather_now, temperature, wind, date_now, time)"
                    f" VALUES ('{data.cityname}',"
                    f"'{main_weather_now}','{temp}','{wind}', '{inserted_dt}', '{inserted_time}')"
                )
            )
            res = {
                "city": data.cityname,
                "main_weather_now": main_weather_now, 
                "temperature": temp,
                "wind": wind,
                "date_now": inserted_dt,
            }
            return res
