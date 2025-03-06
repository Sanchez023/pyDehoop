import pandas 
from Dehoop import Dehoop
from utils.TransFormer import GetFieldInfosFromJS

dfs = pandas.read_excel("C:/Xiaomi Cloud/Desktop/fieldStandars.xlsx",header=0)

data_frame = dfs
data_frame = data_frame[["标准分类","来源字段名","标准英文名称","标准中文名"]].fillna("")

# d = Dehoop("10.1.8.17",30104)
# d.Login(username="liub",passwd="hbbx@2024SxdC")
# d.DownloadStandars(projectName="中台-客户管理其他域模型开发")
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
