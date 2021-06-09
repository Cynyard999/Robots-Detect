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


def siftRecord(line):
    if line['cart'] + line['favor'] + line['getDetail'] + line['login'] < 3 and line['buy'] > 10:
        return True
    if line['cart'] + line['favor'] + line['getDetail'] + line['login'] < 1 and line['buy'] <= 10 and line['buy'] > 5:
        return True
    return False


def get_robot_list(data):
    q1, q3 = data['buy'].quantile([0.05, 0.95])
    iqr = q3 - q1
    suspicious_list = data[
        (data['buy'] > q3 + iqr * 3)]
    data = suspicious_list.sort_values(by=['buy', 'cart'], ascending=[False, True])
    result = data[data.apply(siftRecord, axis=1)]
    result.to_csv('./data/result/click_farm_robots.csv')
    print("Done")


def start(mongo_collection):
    print("Getting User-Record Cursor...")
    with mongo_collection.aggregate(pipeline, allowDiskUse=True
                                    ) as cursor:
        print("Reading MongoDb...")
        iterateCursor(cursor)
        print("Reading Succeed")
        cursor.close()
    print("Converting Dic to DataFrame...")
    data = pd.DataFrame(userRecords).T
    print("Saving to Csv...")
    data.to_csv('./data/temp/user_records.csv')
    # data = pd.read_csv('./data/temp/user_records.csv', index_col=0)
    get_robot_list(data)
