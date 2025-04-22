import pandas
from Dehoop import Dehoop

d = Dehoop("10.1.8.17","30104")
d.Login("liub","hbbx@2024SxdC")
projectName = "公共域模型开发"
workbook_path = "C:/Xiaomi Cloud/Desktop/Mappingconfig.xlsx"

OutlineWorks = d.QueryOutLineWorks(projectName)
for id in OutlineWorks:
    if OutlineWorks[id]["type"] in ("DDL","SPARK"):
        print(d.GetScript(projectName,id))
        break
# df = pandas.read_excel(workbook_path)
