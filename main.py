import pymongo
import pandas as pd
from pandas import DataFrame

import crawler_detect
import click_farm_detect
import order_grab_detect
import credential_stuff_detect

MONGO_URL = "mongodb://localhost:27017/"
MONGO_DB_NAME = "user_logs"
MONGO_DB_COLLECTION = "samples"

mongo_client = pymongo.MongoClient(MONGO_URL)

mongo_db = mongo_client[MONGO_DB_NAME]

mongo_collection = mongo_db[MONGO_DB_COLLECTION]





if __name__ == "__main__":
    crawler_detect.start(mongo_collection)
    # click_farm_detect.start(mongo_collection)
    # order_grab_detect.start(mongo_collection)
    # credential_stuff_detect.start(mongo_collection)
