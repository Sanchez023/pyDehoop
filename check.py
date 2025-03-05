import pandas 
from pandas import DataFrame
from utils.TransFormer import GetFieldInfosFromJS

dfs = pandas.read_excel("C:/Xiaomi Cloud/Desktop/fieldStandars.xlsx",header=0)

data_frame = dfs
data_frame = data_frame[["标准分类","来源字段名","标准英文名称","标准中文名"]].fillna("")

dict_fields = GetFieldInfosFromJS()
for field in data_frame.values:
   
    clsname = field[0]
    cname = field[1]
    code = field[2]
    standarField = field[3]

    fieldInfo: dict = dict_fields[clsname]
    exist = fieldInfo.get(code)
    if not exist:
        print(f"{clsname},{code},{cname},{standarField}")
