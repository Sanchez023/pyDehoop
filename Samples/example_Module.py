from Dehoop import Dehoop
import pandas 
from pandas import DataFrame
from ParamStruct import ParamEntitry
# d = Dehoop("192.168.16.100","30104")
d = Dehoop("10.1.8.17","30104")

# d.Login("hbbxlb","hbbx@2024")

d.token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJkZWhvb3B1c2VyaWQiOiI2NTcyNTM5NTk3NjQ4MDM1ODQiLCJ0ZW5hbnRpZCI6IjY1NjYwOTExODM5OTc1ODMzNiIsImV4cCI6MTc0MTI1MzI0NSwiaWF0IjoxNzQwOTk0MDQ1fQ.SnfX9ygULzs-VEY0-iYtNnAJgy-92alKh0R0OF2s1cifamwPU1eSS8VAa86CBJUbuI0YKPaIm3H7x3nkCqVt8Q"
d.tenantid = "656609118399758336"
projectName = "农险统计分析项目"
d.DownloadStandars(projectName)
dfs = pandas.read_excel("./File/批量建模模板.xlsx",sheet_name=None,header=None)


dataLayerId = d.GetDataLayers(projectName)["ODS"]["id"]
memorySpaceId = d.GetSpacesInfo(projectName)['测试开发存储空间']
busniessType = d.GetBusinessProcesses(projectName)["ReadMe"]

for sheetName in dfs:
    df:DataFrame= dfs[sheetName]
    df.columns = [i for i in range(0,len(df.columns))]
    colums = df.iloc[3].fillna("remark").tolist()
    tableCName = df.iloc[0,2]
    tableName = df.iloc[1,2]
    desc = df.iloc[2,2]

    data_frame = df.iloc[4:,:].reset_index(drop=True)
    data_frame.columns= colums
    # print(data_frame[0:2])
    fields = {}
    data_frame = data_frame[["标准字段分类","源表字段中文名","标准物理字段","主键","外键"]].fillna("")
    
    p = ParamEntitry(tableCName,tableName,"ATOMIC_TRANSACTIONS",desc,dataLayerId,memorySpaceId,businessType=busniessType)
    id = d.CreateEntity(projectName,p)

    for i in data_frame.values:
        isPK = False if i[3] == "" else True
        isFK = False if i[4] == "" else True
        fields[i[2]] = {"clsName":i[0],"cname":i[1],"isPK":isPK,"isFK":isFK}
    
    d.SaveEntitryFields(projectName,id,fields)
    break