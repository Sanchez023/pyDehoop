from Dehoop import Dehoop
import pandas 
from pandas import DataFrame
d = Dehoop("192.168.16.100","30104")

# d.Login("hbbxlb","hbbx@2024")

d.token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJkZWhvb3B1c2VyaWQiOiI2NDM3NDExNDYwNDgxMDI0MDAiLCJ0ZW5hbnRpZCI6IjY0MjQxNzI4MzAxMTk2OTAyNCIsImV4cCI6MTc0MDk5MjYyMCwiaWF0IjoxNzQwNzMzNDIwfQ.MSwgrG8UNykPZ5ZfFB-l90tjbVKz_3KNWpm2X_xSeJ3J3FMEG2xNJjIl02ToYqPvgCH557-SIznBqQf3PIxhhA"
d.tenantid = "642417283011969024"
d.tenantid = "643741146048102400"

dfs = pandas.read_excel("C:/Xiaomi Cloud/Desktop/book1.xlsx",sheet_name=None,header=None)

for sheetName in dfs:
    df:DataFrame= dfs[sheetName]
    df.columns = [i for i in range(0,len(df.columns))]
    colums = df.iloc[:,4]
    tableCName = df.iloc[0,2]
    tableName = df.iloc[1,2]
    print(tableCName)
    print(tableName)
    print(colums)
    # data_df:DataFrame = df