import time
import heapq
from bson.son import SON
import pandas as pd

TIME_INTERVAL = 60  # seconds
pipeline = [
    {'$sort': SON([("requestBody.userId", 1), ("date", 1)])}
]

# example = {"1000":{"max_get_detail_per_min":100}}
user_get_detail_frequency = {}

heap = []


# t2-t1
def computeTimeDifference(t1: str, t2: str):
    sec1 = time.mktime(time.strptime(t1, "%Y-%m-%d %X"))
    sec2 = time.mktime(time.strptime(t2, "%Y-%m-%d %X"))
    return sec2 - sec1


def iterateCursor(_cursor):
    currentUserId = ""
    # 保存当前user的所有记录
    currentUserRecords = []
    for row in _cursor:
        if row["action"] != "getDetail":
            continue
        if currentUserId == "":
            currentUserRecords = [row]
            currentUserId = row["requestBody"]["userId"]
            continue
        if row["requestBody"]["userId"] != currentUserId:
            res = handleUserRecords(currentUserRecords)
            # print(currentUserId, res)
            user_get_detail_frequency[currentUserId] = {'max_get_detail_per_min': res}
            # 最小堆得到最大的10个元素
            if len(heap) < 10:
                heapq.heappush(heap, res)
            else:
                # 压入 然后弹出最小的元素
                heapq.heappushpop(heap, res)
            # refresh
            currentUserRecords = [row]
            currentUserId = row["requestBody"]["userId"]
        else:
            currentUserRecords.append(row)


def handleUserRecords(userGetDetailRecords):
    if not userGetDetailRecords:
        return

    maxFrequency = -1

    # 窗口内的最大时间差是否为规定时间间隔 True：是 False：否
    def isValid(window):
        firstRecord = window[0]
        lastRecord = window[-1]
        if computeTimeDifference(firstRecord["date"], lastRecord["date"]) > TIME_INTERVAL:
            return False
        return True

    left, right = 0, 0
    win = []
    # 滑动窗口
    while right < len(userGetDetailRecords):
        win.append(userGetDetailRecords[right])
        right += 1
        while not isValid(win):
            win.pop(0)
            left += 1
        maxFrequency = maxFrequency if maxFrequency > len(win) else len(win)

    return maxFrequency


def siftCrawler(line, user_records):
    user_id = line.name
    # loc[[user_id,...]]返回dataframe再取值需要加values(?)，loc[user_id]返回series
    user_record = user_records.loc[int(user_id)]
    if user_record["cart"] == 0 and user_record["favor"] == 0 and user_record["buy"] == 0 and user_record[
        "getDetail"] > 100:
        return True
    return False


def get_crawler(data):
    user_records = ''
    try:
        user_records = pd.read_csv('data/temp/user_records.csv', index_col=0)
    except:
        print('错误：请先运行click_farm_detect.start以获取中间数据！')
        return
    q1, q3 = data['max_get_detail_per_min'].quantile([0.25, 0.75])
    iqr = q3 - q1
    suspicious_list = data[
        (data['max_get_detail_per_min'] > q3 + iqr * 3) | (data['max_get_detail_per_min'] < q1 - iqr * 3)]
    suspicious_list.to_csv('./data/temp/suspicious_crawler.csv')
    crawler_list = suspicious_list[suspicious_list.apply(siftCrawler, axis=1, user_records=user_records)]
    crawler_list.to_csv("./data/result/crawler_robots.csv")
    print("Done")


def start(mongo_collection):
    print("Getting Cursor...")
    with mongo_collection.aggregate(pipeline, allowDiskUse=True
                                    ) as cursor:
        print("Reading MongoDb...")
        iterateCursor(cursor)
        print("Reading Succeed")
        cursor.close()
    print("Converting Dic to DataFrame...")
    data = pd.DataFrame(user_get_detail_frequency).T
    print("Saving to Csv...")
    data.to_csv('./data/temp/user_get_detail_frequency.csv')
    # 如果嫌调试太慢 可以直接读之前跑好的中间结果
    # data = pd.read_csv('./data/temp/user_get_detail_frequency.csv', index_col=0)
    print("Getting Suspicious List")
    get_crawler(data)
