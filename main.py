import databases
import sqlalchemy
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from math import cos, asin


DATABASE_URL = "sqlite:///./sqlite.db"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

addrss_book = sqlalchemy.Table(
    "address_book",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("address", sqlalchemy.String),
    sqlalchemy.Column("phone", sqlalchemy.String),
    sqlalchemy.Column('latitude', sqlalchemy.Float),
    sqlalchemy.Column('longitude', sqlalchemy.Float),
)


engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
metadata.create_all(engine)


class AddressBookIn(BaseModel):
    name: str
    address: str
    phone: str
    latitude: float
    longitude: float


class AddressBook(BaseModel):
    id: int
    name: str
    address: str
    phone: str
    latitude: float
    longitude: float


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.post("/address/", response_model=AddressBook)
async def create_address(address: AddressBookIn):
    query = addrss_book.insert().values(
        name=address.name, address=address.address, phone=address.phone, latitude=address.latitude, longitude=address.longitude
    )
    last_record_id = await database.execute(query)
    return {**address.dict(), "id": last_record_id}


@app.put("/address/{address_id}", response_model=AddressBook)
async def update_address(address_id: int, address: AddressBookIn):
    query = (
        addrss_book.update()
        .where(addrss_book.c.id == address_id)
        .values(
            name=address.name,
            address=address.address,
            phone=address.phone,
            latitude=address.latitude,
            longitude=address.longitude
        )
    )
    await database.execute(query)
    return {**address.dict(), "id": address_id}


@app.delete("/address/{address_id}", response_model=AddressBook)
async def delete_address(address_id: int):
    query = addrss_book.delete().where(addrss_book.c.id == address_id)
    await database.execute(query)
    return {"id": address_id}


def distance_between_two_points(lat1, lon1, distance):
    R = 6371
    dLat = distance / R
    dLon = distance / (R * cos(asin(lat1)))
    lat2 = lat1 + dLat * 180 / 3.141592653589793
    lon2 = lon1 + dLon * 180 / 3.141592653589793
    return lat2, lon2


@app.get("/address-by-distance/", response_model=List[AddressBook])
async def get_address_by_distance(lat: float, lon: float, distance: int):
    query = addrss_book.select()
    rows = await database.fetch_all(query)
    result = []
    for row in rows:
        if distance_between_two_points(lat, lon, distance) == (row['latitude'], row['longitude']):
            result.append(row)
    return result
