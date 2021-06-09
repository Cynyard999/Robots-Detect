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


def siftUser(line, data2):
    # 规定失败次数小于等于5或者成功次数大于1，都是正常行为
    if line['fail_count'] <= 5 or line['succeed_count'] > 1:
        return False
    # 当这个ip一次也没成功的时候
    if line['succeed_count'] == 0:
        fail_ip_list = line['fail_ip_list']
        # 始终只有一个ip在尝试登陆，并且一直失败：撞库失败
        if len(list(set(fail_ip_list))) == 1:
            ip_record_line = data2.loc[[fail_ip_list[0]]]
            # values[0]是从series中取得值
            succeed_ip_record = ip_record_line['succeed_user_list'].values[0]
            fail_ip_record = ip_record_line['fail_user_list'].values[0]
            # 并且这个ip成功登陆了三个及以上的user或者登陆失败了三个及以上的user
            if len(list(set(succeed_ip_record))) > 2 or len(list(set(fail_ip_record))) > 2:
                return True
    # 当这个ip最终撞库成功了
    if line['succeed_count'] == 1:
        fail_ip_list = line['fail_ip_list']
        succeed_ip_list = line['succeed_ip_list']
        # 始终只有一个ip在尝试登陆，并且只成功了一次：撞库成功
        if len(list(set(fail_ip_list))) == 1 and len(list(set(succeed_ip_list))) == 1 and fail_ip_list[0] == \
                succeed_ip_list[0]:
            ip_record_line = data2.loc[fail_ip_list[0]]
            succeed_ip_record = ip_record_line['succeed_user_list']
            fail_ip_record = ip_record_line['fail_user_list']
            # 并且这个ip成功登陆了三个及以上的user或者登陆失败了三个及以上的user
            if len(list(set(succeed_ip_record))) > 2 or len(list(set(fail_ip_record))) > 2:
                return True
    return False


def get_credential_stuffing_robots(data1, data2):
    data1["succeed_count"] = data1[["succeed_ip_list"]].apply(lambda x: len(x["succeed_ip_list"]), axis=1)
    data1["fail_count"] = data1[["fail_ip_list"]].apply(lambda x: len(x["fail_ip_list"]), axis=1)
    data1 = data1[data1.apply(siftUser, axis=1, data2=data2)]
    # 取得user-ip对应
    result = data1[["fail_ip_list"]].apply(lambda x: x['fail_ip_list'][0], axis=1)
    result.rename('ip',inplace=True)
    # 这里的result是series类型
    result.to_csv('./data/result/credential_stuff_robots.csv')
    print("Done")


def start(mongo_collection):
    print("Getting User-Login Cursor...")
    with mongo_collection.aggregate(pipeline1, allowDiskUse=True
                                    ) as cursor:
        print("Reading MongoDb...")
        iterateUserCursor(cursor)
        print("Reading Succeed")
        cursor.close()
    print("Getting Ip-Login Cursor...")
    with mongo_collection.aggregate(pipeline2, allowDiskUse=True
                                    ) as cursor:
        print("Reading MongoDb...")
        iterateIpCursor(cursor)
        print("Reading Succeed")
        cursor.close()
    print("Converting Dic to DataFrame...")
    data1 = pd.DataFrame(userRecords).T
    data2 = pd.DataFrame(loginRecords).T
    print("Saving to Csv...")
    data1.to_csv('./data/temp/user_ip_list.csv')
    data2.to_csv('./data/temp/ip_user_list.csv')
    print("Sifting...")
    get_credential_stuffing_robots(data1, data2)
