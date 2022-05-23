import pandas as pd
import datetime as dt
import json

result = {}#output結果
nowTime = dt.datetime.now()#現在時間

class execute():
    def outputResult(self):
        df = pd.read_csv('/Users/duckchen/Desktop/Data/ai-alert-internet-2022-05.csv')
        dayArray = [1, 7, 14]
        ipArray=["src_ip", "dst_ip", "alert_id"]
        for i in dayArray:
            new_df = execute().calDays(df, i)
            for j in ipArray:
                execute().arrangeData(j, new_df, "day_"+str(i))

        with open("123.json", "w") as f:
            json.dump(result, f, indent = 4)

    #組裝資料
    def arrangeData(self, colName, df, lastDay):
        ipDict = dict()
        data = df[colName]
        dataIpArray = []#src/dst/alert
        dataDateArray = result.get(lastDay)#X天內
        if dataDateArray == None:
            dataDateArray = []
        #每種ip/id數量
        for name, num in data.value_counts().head(10).iteritems():
            newDict = dict()
            newDict["name"] = name
            newDict["value"] = num
            dataIpArray.append(newDict)
        ipDict[colName] = dataIpArray
        dataDateArray.append(ipDict)
        result[lastDay] = dataDateArray
    
    def calDays(self, df, num):
        timeRange = dt.timedelta(days = num)
        newTime = dt.datetime.strftime(nowTime - timeRange, '%Y-%m-%d %H:%M:%S')
        new_df = df[df['ingest_timestamp'] >= newTime]
        return new_df

execute().outputResult()