import datetime
from bson.son import SON
import pandas as pd


pipeline = [
    {'$sort': SON([("requestBody.userId", 1), ("date", 1)])}
]
ISOTIMEFORMAT = '%Y-%m-%d %X'

# example = {"10000": {"integral_point_buy": 1, "not_integral_point_buy": 2}}
userRecords = {}


def isAroundIntegralPoint(time: str):
    minute = datetime.datetime.strptime(time, ISOTIMEFORMAT).minute
    second = datetime.datetime.strptime(time, ISOTIMEFORMAT).second
    if second == 0 and (minute == 30 or minute == 0):
        return True
    return False


def iterateCursor(_cursor):
    currentUserId = ''
    currentBuyRecord = {}
    for row in _cursor:
        if row["requestBody"]["userId"] != currentUserId:
            currentBuyRecord = {"integral_point_buy": 0, "not_integral_point_buy": 0}
            currentUserId = row["requestBody"]["userId"]
            # 保存当前用户的记录
            userRecords[currentUserId] = currentBuyRecord
        if row["action"] == "buy" and isAroundIntegralPoint(row["date"]):
            currentBuyRecord["integral_point_buy"] += 1
        elif row["action"] == "buy":
            currentBuyRecord["not_integral_point_buy"] += 1


def start(mongo_collection):
    with mongo_collection.aggregate(pipeline, allowDiskUse=True
                                    ) as cursor:
        iterateCursor(cursor)
        cursor.close()
    data = pd.DataFrame(userRecords).T
    return data
