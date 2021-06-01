from bson.son import SON
import pandas as pd


# example_user = {"1000": {"succeed_ip_list": [], "fail_ip_list": []}}
# example_login = {"1.1.1.1": {
#     "succeed_user_list": ["100", "10000"], "fail_user_list": ["111", "2222"]}, "login_count": 0}
pipeline1 = [
    {'$match': {'ip': {'$ne': ''}}},
    {'$sort': SON([("requestBody.userId", 1), ("date", 1)])}
]
pipeline2 = [
    {'$match': {'ip': {'$ne': ''}}},
    {'$sort': SON([("ip", 1), ("date", 1)])}
]
userRecords = {}
loginRecords = {}


def iterateUserCursor(_cursor):
    currentUserId = ''
    currentUserRecord = {}
    for row in _cursor:
        if row["action"] != "login":
            continue
        if row["requestBody"]["userId"] != currentUserId:
            currentUserId = row["requestBody"]["userId"]
            currentUserRecord = {"succeed_ip_list": [], "fail_ip_list": []}
            userRecords[currentUserId] = currentUserRecord
        if row["requestBody"]["success"] == "1":
            currentUserRecord["succeed_ip_list"].append(row["ip"])
        if row["requestBody"]["success"] == "0":
            currentUserRecord["fail_ip_list"].append(row["ip"])


def iterateIpCursor(_cursor):
    currentIp = ''
    currentIpRecord = {}
    for row in _cursor:
        if row["action"] != "login":
            continue
        if row["ip"] != currentIp:
            currentIp = row["ip"]
            currentIpRecord = {"succeed_user_list": [], "fail_user_list": []}
            loginRecords[currentIp] = currentIpRecord
        if row["requestBody"]["success"] == "1":
            currentIpRecord["succeed_user_list"].append(row["requestBody"]["userId"])
        elif row["requestBody"]["success"] == "0":
            currentIpRecord["fail_user_list"].append(row["requestBody"]["userId"])


def start(mongo_collection):
    with mongo_collection.aggregate(pipeline1, allowDiskUse=True
                                    ) as cursor:
        iterateUserCursor(cursor)
        cursor.close()
    with mongo_collection.aggregate(pipeline2, allowDiskUse=True
                                    ) as cursor:
        iterateIpCursor(cursor)
        cursor.close()
    data1 = pd.DataFrame(userRecords).T
    data2 = pd.DataFrame(loginRecords).T
    return data1, data2
