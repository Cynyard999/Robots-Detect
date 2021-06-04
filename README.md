# 恶意机器人判定

## 前期配置

### 数据接收过程

### 数据库配置

基于不同类型的数据有不同key-value对，通过**text2json2mongo.py**文件，我们选择将数据存入MongoDb数据库中，基于数据库和电脑性能的考虑，我们从8100w数据中选取100w数据进行分析，数据格式见文末

## 机器人

### 刷单机器人

#### 特征

同一个用户大量购买商品，并且除了buy之外的行为都很少，或者没有

#### 识别过程

通过aggregate函数将数据库的数据按照userID排序以便于分用户遍历，并返回可以遍历的游标。

遍历游标得到每个用户的行为统计，存为userRecords字典，并保存为中间文件

```python
with mongo_collection.aggregate(pipeline, allowDiskUse=True
                               ) as cursor:
  iterateCursor(cursor)
  cursor.close()
  data = pd.DataFrame(userRecords).T
  data.to_csv('./data/temp/user_records.csv')
```

筛选数据，设置刷单机器人的行为阈值

```python
result = data[data.apply(siftRecord, axis=1)]
def siftRecord(line):
    if line['buy'] >= 20 and line['cart'] + line['favor'] + line['getDetail'] < 3:
        return True
    return False
```

### 爬虫机器人

#### 特征

同一个userID，短时间内，对于很多个商品进行getDetail，但是其他行为例如添加购物车，收藏等数量很少，或者没有。

#### 识别过程

通过aggregate函数将数据库的数据按照userID排序以便于分用户遍历，并返回可以遍历的游标。

```python
with mongo_collection.aggregate(pipeline, allowDiskUse=True
                               ) as cursor:
  print("Reading MongoDb...")
  iterateCursor(cursor)
  print("Reading Succeed")
  cursor.close()
```

遍历游标，并通过滑动窗口的方式得到每个用户在这段时间内的每一分钟的最大getDetail数量，通过字典存取，依赖hash原理可以更快获得数据。

```python
# 滑动窗口
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
```

将所有用户的字典记录转化为Pandas的DataFrame，并存为中间文件，再利用DataFrame进行进一步的筛选

```python
# 使用四分位法得到显著异常的用户频率
q1, q3 = data['max_get_detail_per_min'].quantile([0.25, 0.75])
    iqr = q3 - q1
    suspicious_list = data[
        (data['max_get_detail_per_min'] > q3 + iqr * 3) | (data['max_get_detail_per_min'] < q1 - iqr * 3)]
# 通过与刷单机器人得到的用户行为记录做比较，筛选出满足特征的用户，设定userFrequency阈值为100
def siftCrawler(line, user_records):
    user_id = line.name
    # loc[[user_id,...]]返回dataframe再取值需要加values(?)，loc[user_id]返回series
    user_record = user_records.loc[int(user_id)]
    if user_record["cart"] == 0 and user_record["favor"] == 0 and user_record["buy"] == 0 and user_record[
        "getDetail"] > 100:
        return True
    return False

```

结果存储为csv文件

```python
crawler_list = suspicious_list[suspicious_list.apply(siftCrawler, axis=1, user_records=user_records)]
crawler_list.to_csv("./data/result/crawler_robots.csv")
```

### 抢单机器人

#### 特征

只在靠近整点并整点过后的时候购买的userId

对于整点抢购行为，秒杀的成功率异常偏高的userId

#### 过程

通过aggregate函数将数据库的数据按照userID排序以便于分用户遍历，并返回可以遍历的游标。

遍历游标得到每个用户的筛选后的购买行为统计，存为userRecords字典，并保存为中间文件

```python
with mongo_collection.aggregate(pipeline, allowDiskUse=True
                               ) as cursor:
    iterateCursor(cursor)
    cursor.close()
    data = pd.DataFrame(userRecords).T
    data.to_csv('./data/temp/user_buy_records.csv')
```

【定义】在整点前后1min之内的购买行为被认为是整点操作

```python
def isAroundIntegralPoint(time: str):
    minute = datetime.datetime.strptime(time, ISOTIMEFORMAT).minute
    # second = datetime.datetime.strptime(time, ISOTIMEFORMAT).second
    # 考虑整点范围两分钟内作为整点整的依据:xx:59:00~xx:00:59
    if minute >= 59 or minute < 1:
        return True
    return False
```

中间文件格式说明

- integral_point_buy：整点购买行为
- ipb_rate：整点购买行为占所有购买行为比例
- kill_rate：整点行为中秒杀商品的比率

筛选数据，设置抢单机器人的行为阈值：整点购买行为多于5次，ipb_rate>0.8，kill_rate>0.9

```python
def siftRecord(line):
    if line['integral_point_buy'] >= 5 
    		and line['ipb_rate'] >= 0.8 
        	and line['kill_rate'] > 0.9:
        return True
    return False
```

将结果排序后存为csv

```python
data = data.sort_values(by=['integral_point_buy', 'ipb_rate', 'kill_rate'], ascending=[False, False, False])
result = data[data.apply(siftRecord, axis=1)]
result.to_csv('./data/result/order_grab_robots.csv')
```

### 撞库机器人

#### 特征

同一个ip进行大量的不同账号的登陆操作，并且失败次数远大于成功次数

同一个账号在同一个ip下进行大量的登陆操作，并且失败次数远大于成功次数，并且没有其他行为例如添加购物车等

#### 过程

对同一个user，记录登陆成功和登陆失败的ip字典为userRecords

对同一个ip，记录登陆成功和登陆失败的user字典为loginRecords

```python
with mongo_collection.aggregate(pipeline1, allowDiskUse=True
                               ) as cursor:
  iterateUserCursor(cursor)
  cursor.close()
with mongo_collection.aggregate(pipeline2, allowDiskUse=True
                                 ) as cursor:
  iterateIpCursor(cursor)
  cursor.close()
```

转化为DataFrame并存入中间文件，并对DataFrome进行筛选

```python
data1 = pd.DataFrame(userRecords).T
data2 = pd.DataFrame(loginRecords).T
...
data1 = data1[data1.apply(siftUser, axis=1, data2=data2)]
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
```

将结果存为csv

```python
result.to_csv('./data/result/credential_stuff_robots.csv')
print("Done")
```

## 数据库格式

```
{
	"id":""
	"date":""
	"action":"getDetail"
	"ip":""
	"requestBody":{
		"userId":"",
		"itemId":"",
		"categoryId":""
	}
}
{
	"id":""
	"date":""
	"action":"buy"
	"ip":""
	"requestBody":{
		"userId":"",
		"itemId":"",
		"categoryId":""
		"isSecondKill":""
	}
}
{
	"id":""
	"date":""
	"action":"cart"
	"ip":""
	"requestBody":{
		"userId":"",
		"itemId":"",
		"categoryId":""
	}
}
{
	"id":"",
	"date":"",
	"action":"login",
	"ip":"1111",
	"requestBody":{
		"userId":"",
		"password":"",
		"authCode":""
		"success":""
	}
}
{
	"id":"",
	"date":"",
	"action":"favor",
	"ip":"",
	"requestBody":{
		"userId":"",
		"itemId":"",
		"categoryId":""
	}
}
```



