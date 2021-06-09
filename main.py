import pymongo

import crawler_detect
import click_farm_detect
import order_grab_detect
import credential_stuff_detect

MONGO_URL = "mongodb://localhost:27017/"
MONGO_DB_NAME = "user_logs"
# MONGO_DB_COLLECTION = "flow_data"
MONGO_DB_COLLECTION = "samples"

mongo_client = pymongo.MongoClient(MONGO_URL)

mongo_db = mongo_client[MONGO_DB_NAME]

mongo_collection = mongo_db[MONGO_DB_COLLECTION]

if __name__ == "__main__":
    # 刷单机器人
    print("Beginning Click-Farm Detect")
    click_farm_detect.start(mongo_collection)
    # 撞库机器人
    print("Beginning Credential_Stuff Detect")
    credential_stuff_detect.start(mongo_collection)
    # 爬虫机器人
    print("Beginning Credential_Stuff Detect")
    crawler_detect.start(mongo_collection)
    # 抢单机器人
    print("Beginning Order_Grab Detect")
    order_grab_detect.start(mongo_collection)
