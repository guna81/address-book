# import required packages
import uvicorn
import databases
import sqlalchemy
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from math import cos, asin


# create a database connection
DATABASE_URL = "sqlite:///./sqlite.db"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()


# create a table
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


# create a model
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


# create a FastAPI app
app = FastAPI()


# connect the app to the database
@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/")
def root():
    return {"message": "Welcome to the address book API"}

# create a route to get all the records

# address create endpoint
@app.post("/address/", response_model=AddressBook)
async def create_address(address: AddressBookIn):
    query = addrss_book.insert().values(
        name=address.name, address=address.address, phone=address.phone, latitude=address.latitude, longitude=address.longitude
    )
    last_record_id = await database.execute(query)
    return {**address.dict(), "id": last_record_id}


# address update endpoint
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


# address delete endpoint
@app.delete("/address/{address_id}", response_model=AddressBook)
async def delete_address(address_id: int):
    query = addrss_book.delete().where(addrss_book.c.id == address_id)
    await database.execute(query)
    return {"id": address_id}


# get all addresses endpoint
@app.get("/address/", response_model=List[AddressBook])
async def get_addresses():
    query = addrss_book.select()
    return await database.fetch_all(query)


# distance calculation function
def distance_between_two_points(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2 - lat1) * p) / 2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(a)


# address by distance endpoint
@app.get("/address-by-distance/", response_model=List[AddressBook])
async def get_address_by_distance(lat: float, lon: float, distance: int):
    query = addrss_book.select()
    addresses = await database.fetch_all(query)
    addresses = [dict(address) for address in addresses]
    addresses = [
        address
        for address in addresses
        if distance_between_two_points(lat, lon, address["latitude"], address["longitude"]) <= distance
    ]
    return addresses



# run the app
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)