import pandas as pd


# example = {
#     "1000": {"max_get_detail_per_min": 100},
#     "2000": {"max_get_detail_per_min": 200},
#     "3000": {"max_get_detail_per_min": 300},
# }

def getLen(row):
    a = row['a']
    return len(a)


example = {"1.1.1.1": {
    "succeed_user_list": ["100", "10000"], "fail_user_list": ["111", "2222"], "login_count": 0}}

data = pd.DataFrame(example).T
data.columns = ['a', 'b', 'c']
# data["succeed_count"] = data[["a"]].apply(getLen, axis=1)
data["succeed_count"] = data[["a"]].apply(lambda x: len(x["a"]), axis=1)
print(data)
