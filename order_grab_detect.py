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
    # second = datetime.datetime.strptime(time, ISOTIMEFORMAT).second
    # 考虑整点范围两分钟内作为整点整的依据:xx:59:00~xx:00:59
    if minute >= 59 or minute < 1:
        return True
    return False


def iterateCursor(_cursor):
    currentUserId = ''
    currentBuyRecord = {}
    buy_count = 0
    kill = 0
    for row in _cursor:
        # 只考虑buy行为
        if row["action"] != "buy":
            continue
        if row["requestBody"]["userId"] != currentUserId:
            if currentUserId != '':
                # 计算整点频率和整点中秒杀频率
                if buy_count != 0:
                    currentBuyRecord["ipb_rate"] = round(currentBuyRecord["integral_point_buy"] / buy_count, 3)
                if currentBuyRecord["integral_point_buy"] != 0:
                    currentBuyRecord["kill_rate"] = round(kill / currentBuyRecord["integral_point_buy"], 3)
            buy_count = 0
            kill = 0
            currentBuyRecord = {"integral_point_buy": 0, "ipb_rate": 0.0, "kill_rate": 0.0}
            currentUserId = row["requestBody"]["userId"]
            # 保存当前用户的记录
            userRecords[currentUserId] = currentBuyRecord
        if isAroundIntegralPoint(row["date"]):
            currentBuyRecord["integral_point_buy"] += 1
            if row["requestBody"]["isSecondKill"] == "1":
                kill += 1
        buy_count += 1


def siftRecord(line):
    if line['integral_point_buy'] >= 5 and line['ipb_rate'] >= 0.8 and line['kill_rate'] > 0.9:
        return True
    return False


def get_order_grab_robots(data):
    data = data.sort_values(by=['integral_point_buy', 'ipb_rate', 'kill_rate'], ascending=[False, False, False])
    result = data[data.apply(siftRecord, axis=1)]
    result.to_csv('./data/result/order_grab_robots.csv')
    print("Done")


def start(mongo_collection):
    with mongo_collection.aggregate(pipeline, allowDiskUse=True
                                    ) as cursor:
        iterateCursor(cursor)
        cursor.close()
    data = pd.DataFrame(userRecords).T
    print("Saving to Csv...")
    data.to_csv('./data/temp/user_buy_records.csv')
    print("Getting possible order grab robots list...")
    get_order_grab_robots(data)
