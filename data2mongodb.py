import os
from typing import Optional
from pymongo import MongoClient
import pymongo

from spatialpandas import io
from spatialpandas import GeoDataFrame

from dotenv import load_dotenv

load_dotenv(".gtfs_builder.env")


from functools import wraps
from time import time

def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print('func:%r args:[%r, %r] took: %2.4f sec' % (f.__name__, args, kw, te-ts))
        return result
    return wrap




def get_data(study_area):
    return GeoDataFrame(io.read_parquet(
            f"data\{study_area}_moving_stops.parq",
            columns=["start_date", "end_date", "x", "y", "geometry", "route_long_name", "route_type"]).astype({
            "start_date": "uint32",
            "end_date": "uint32",
            "geometry": "Point[float64]",
            "x": "category",
            "y": "category",
            "route_type": "category",
            "route_long_name": "category",
        })
    )

data = {
    study_area: {
        "data": get_data(study_area),
        "study_area": study_area
    }
    for study_area in os.environ["AREAS"].split(",")
}


class MongoDbHelper:

    __SERVERSELECTIONTIMEOUTMS = 10
    __CONNECTTIMEOUTMS = 20000
    __FAKE_DATA = {"fake": 'data'}

    __slots__ = (
        "client",
        "db",
        "objects",
    )

    def __init__(self, credentials: str) -> None:
        self.client = MongoClient(
            credentials,
            serverSelectionTimeoutMS=self.__SERVERSELECTIONTIMEOUTMS,
            connectTimeoutMS=self.__CONNECTTIMEOUTMS
        )

    def drop_database(self, db_name: str) -> None:
        self.client.drop_database(db_name)

    def create_database(self, db_name: str, overwrite: bool = False):
        if self._check_if_database_exists(db_name):
            if overwrite:
                self.drop_database(db_name)
                db_built = self.client[db_name]
                db_built["_temp"].insert_one(self.__FAKE_DATA)
                print(f"Database {db_name} recreated !")
            else:
                raise Exception(f"{db_name} exists!")
        else:
            _ = self.client[db_name]
            print(f"Database {db_name} created !")

        return self.get_db(db_name)

    def get_db(self, db_name: str) -> any:
        # TODO database is not created until you do one insert, create a fake collection ?
        if self._check_if_database_exists(db_name):
            return self.client[db_name]
        raise Exception(f"{db_name} not found!")

    def create_collection_from_db(self, db_name: str, collection_name: str, overwrite: bool = False):
        db = self.get_db(db_name)
        if self._check_if_collection_exists(db, collection_name):
            if overwrite:
                collection = self.get_collection(db_name, collection_name)
                collection.drop()
                _ = db[collection_name]
                print(f"Collection '{collection_name}' from Database {db_name} recreated !")
            else:
                raise Exception(f"{collection_name} on {db_name} database exists!")
        else:
            collection_built = db[collection_name]
            collection_built.insert_one(self.__FAKE_DATA)

            print(f"Collection '{collection_name}' from Database {db_name} created !")

        return self.get_collection(db_name, collection_name)

    def get_collection(self, db_name: str, collection_name: str):
        # TODO database is not created until you do one insert, impact here also ; create a fake data collection ?
        db = self.get_db(db_name)
        if self._check_if_collection_exists(db, collection_name):
            return db[collection_name]
        raise Exception(f"{collection_name} from {db_name} database not found!")

    def _check_if_database_exists(self, db_name: str):
        database_found = self.__find_element_from_list(db_name, self.client.list_database_names())
        if len(database_found) == 1:
            return True
        return False

    def _check_if_collection_exists(self, db, collection_name: str):
        collection_found = self.__find_element_from_list(collection_name, db.list_collection_names())
        if len(collection_found) == 1:
            return True
        return False

    @staticmethod
    def __find_element_from_list(name_value: str, mongo_features_list) -> list:
        return list(filter(lambda x: x == name_value, mongo_features_list))


session = MongoDbHelper(f"mongodb://{os.environ['USER']}:{os.environ['PSWD']}@localhost:27017/")

# create db
gtfs_builder_db = session.create_database("gtfs_builder", overwrite=True)

# create collection area
area_bounds_collection = session.create_collection_from_db("gtfs_builder", "area_bounds_collection")

area_bounds_collection = gtfs_builder_db.bounds
area_bounds_collection.create_index('area', unique=True)


@timing
def insert_data(study_area):
    data = get_data(study_area)

    area_bounds_collection.insert_one({
        "area": data.geometry.total_bounds
    })

    geodata_collection = session.create_collection_from_db("gtfs_builder", study_area)
    geodata_collection.create_index([
        ('start_date', pymongo.ASCENDING),
        ('end_date', pymongo.ASCENDING)],
        name='validity_range'
    )

    geodata_collection.insert_many(
        data[["start_date", "end_date", "x", "y", "route_long_name", "route_type"]].to_dict("records"))
    assert True



for study_area in os.environ["AREAS"].split(","):
    insert_data(study_area)

# a = list(geodata_collection.find({ "start_date" : { "$lte" : 1639973635 }, "end_date" :{ "$gte" : 1639973635 }}))
