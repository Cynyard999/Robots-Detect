import json
import pymongo

MONGO_URL = "mongodb://localhost:27017/"
MONGO_DB_NAME = "user_logs"
MONGO_DB_COLLECTION = "samples"


mongo_client = pymongo.MongoClient(MONGO_URL)

mongo_db = mongo_client[MONGO_DB_NAME]

mongo_collection = mongo_db[MONGO_DB_COLLECTION]

fileIndex = 0

while fileIndex <= 0:
    # json_file = open("./data/log-{0}.json".format(fileIndex), "w")
    with open("./data/log-{0}.txt".format(fileIndex), "r") as text_file:
        # print("load /data/log-{0}.txt".format(fileIndex))
        text_file.seek(0, 2)
        file_size = text_file.tell()
        text_file.seek(0, 0)
        line = text_file.readline()
        while line:
            # 把每一行切分为list
            line = line[1:-2].replace("\'", "")
            line = line.split(", ")
            log = {"id": line[0], "date": line[1], "action": line[2], "ip": "", "requestBody": eval(line[3])}
            if len(line) == 5:
                ip = line[4]
                log["ip"] = ip
            # json_log = json.dumps(log, indent=4, separators=(',', ': '))
            # json_file.write(json_log)
            mongo_collection.insert_one(log)
            print("Progress "+str(fileIndex)+" {:2.1%}".format(text_file.tell() / file_size))
            line = text_file.readline()
    text_file.close()
    # json_file.close()
    fileIndex = fileIndex + 1
