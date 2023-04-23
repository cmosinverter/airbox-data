
import pandas as pd
import json
from dynamodb_json import json_util as dyjson
dropcols=['disposed','push_time','puuid','p_time','lat','lon','setting']

def deserializer(x):
    l=dyjson.loads(x)
    d={}
    for item in l:
        d[item["name"]]=item["value"]
        d[f"${item['name']}__cfg"]=item["config"]
    return d

csv_path='C:\\Users\\Enosim\\.conda\\envs\\Airbox\\Project\\airbox_anaysis_tools\\main\\deviceData\\c92494ea-5e44-4507-8776-984705d1fd2b_2022-12_v0\\1670976000.0.csv'
df=pd.read_csv(csv_path)
for dropcol in dropcols:
    df.drop(dropcol, axis=1, inplace=True)
df["sensors"].map(deserializer)
df2=pd.DataFrame(df["sensors"].map(deserializer).tolist())
df.drop('sensors', axis=1, inplace=True)
res=pd.concat([df,df2], join='outer',axis=1)
print(res)
res.to_csv('ARXEMSE010002_2022_03_format.csv')

