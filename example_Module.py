from Dehoop import Dehoop
import pandas 
from pandas import DataFrame
from ParamStruct import ParamEntitry
d = Dehoop("192.168.16.100","30104")

# d.Login("hbbxlb","hbbx@2024")

d.token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJkZWhvb3B1c2VyaWQiOiI2NDM3NDExNDYwNDgxMDI0MDAiLCJ0ZW5hbnRpZCI6IjY0MjQxNzI4MzAxMTk2OTAyNCIsImV4cCI6MTc0MTI0Mzg0NCwiaWF0IjoxNzQwOTg0NjQ0fQ.MIfoY1xC8I0sQJcDHvTdWhUdEUIx9gsnuuUoOUbS-zn89-WqDdN9JLHF9Et58MCL9hgQFzu9QUV4eCefgLeb7Q"
d.tenantid = "642417283011969024"
projectName = "API项目"
dfs = pandas.read_excel("./File/批量建模模板.xlsx",sheet_name=None,header=None)


dataLayerId = d.GetDataLayers(projectName)["ODS"]["id"]
memorySpaceId = d.GetSpacesInfo(projectName)['测试开发存储空间']
busniessType = d.GetBusinessProcesses(projectName)["技术验证"]

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
    data_frame = data_frame[["标准字段分类","标准物理字段","主键","外键"]].fillna("")
    
    p = ParamEntitry(tableCName,tableName,"ATOMIC_TRANSACTIONS",desc,dataLayerId,memorySpaceId,businessType=busniessType)
    d.CreateEntity(projectName,p)
    break
    # for i in data_frame.values:
    #     isPK = False if i[2] == "" else True
    #     isFK = False if i[2] == "" else True
    #     fields[i[1]] = {"clsName":i[0],"isPK":isPK,"isFK":isFK}
    # break 
    # # data_df:DataFrame = df
