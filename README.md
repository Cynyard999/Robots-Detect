# 恶意机器人判定

## 机器人分类

### 爬虫机器人

同一个userID，短时间内，对于很多个商品进行getDetail，但是buy的行为很少，或者没有（爬取站点信息）

同一个userID，只有getDetail操作，并且非常频繁（增加站点负载）

活跃时间集中，login时间集中？

### 刷单机器人

同一个用户大量购买一个商品

getDetail时间和下单时间相距很短，并且没有收藏操作？

相同id，多个用户大量购买一个商品？但是刷单是没有id的

![image-20210526105210194](/Users/cynyard/Library/Application Support/typora-user-images/image-20210526105210194.png)

### 抢单机器人

只在靠近整点并整点过后的时候购买的userid

（并且在之前没有任何操作）

### 撞库机器人

#### 相同ip多次请求

https://patents.google.com/patent/CN104811449A/zh

接收用户的网络访问请求并解析，以确定其源IP、目的IP、登陆属性信息及用户信息；

配置预设登陆路径和登陆次数阈值，或系统默认内置识别登陆路径的预设格式和登陆次数阈值；

根据目的IP和登陆属性信息、预设登陆路径或登陆路径的预设格式识别是否进行登陆操作，若是，则记录其源IP、目的IP及用户信息；

统计预设时间内同一目的IP的服务器接收到的源IP相同而**用户信息不同**的登陆次数，判断登陆次数是否达到登陆次数阈值，若是，则认定其为撞库攻击行为；若否，则认定其为正常访问行为

https://patents.google.com/patent/CN107347052A/zh

获取预定时间内接收到的登录请求的源IP地址和登录信息；

根据获取到的源IP地址和登录信息，确定所述获取到的源IP地址中具有高频登录行为的源IP地址；

根据所述具有高频登录行为的源IP地址发起的登录请求所使用的密码中具有语义的密码的占比，判断所述源IP地址在所述预定时间内发起的登录请求是否为撞库攻击，其中，所述具有语义的密码为具有语义的概率超过预定概率阈值的密码

#### 一个账号一段时间多次密码

https://dun.163.com/news/p/b8e5e68e4b2c4a25946446655e8663d6

#### 一个账号异地登陆（换ip登陆）







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



