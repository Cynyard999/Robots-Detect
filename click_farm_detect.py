from bson.son import SON
import pandas as pd

# example = {"10000":{"getDetail": 11, "cart": 2, "favor": 3, "login": 2, "buy": 3}}

userRecords = {}
pipeline = [
    {'$sort': SON([("requestBody.userId", 1), ("date", 1)])}
]


def iterateCursor(_cursor):
    currentUserId = ""
    currentUserRecords = {}
    for row in _cursor:
        if currentUserId == "":
            currentUserRecords = {"getDetail": 0, "cart": 0, "favor": 0, "login": 0, "buy": 0}
            currentUserId = row["requestBody"]["userId"]
            currentUserRecords[row["action"]] += 1
            continue
        # 当前用户遍历结束，开始下一个用户的遍历
        if row["requestBody"]["userId"] != currentUserId:
            # 保存当前用户的记录
            userRecords[currentUserId] = currentUserRecords
            # refresh
            currentUserRecords = {"getDetail": 0, "cart": 0, "favor": 0, "login": 0, "buy": 0}
            currentUserId = row["requestBody"]["userId"]
        else:
            currentUserRecords[row["action"]] += 1


def start(mongo_collection):
    with mongo_collection.aggregate(pipeline, allowDiskUse=True
                                    ) as cursor:
        iterateCursor(cursor)
        cursor.close()
    data = pd.DataFrame(userRecords).T
    return data
