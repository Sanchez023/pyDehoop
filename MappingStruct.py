import pandas
from Dehoop import Dehoop

# 初始化Dehoop对象，连接到指定IP和端口
d = Dehoop("10.1.8.17","30104")
# 登录系统，使用指定的用户名和密码
d.Login("liub","hbbx@2024SxdC")
# 设置项目名称
projectName = "公共域模型开发"
# 设置Excel工作簿路径
workbook_path = "C:/Xiaomi Cloud/Desktop/Mappingconfig.xlsx"

# 查询项目中的离线作业
OutlineWorks = d.QueryOutLineWorks(projectName)
# 遍历作业，查找DDL或SPARK类型的作业
for id in OutlineWorks:
    if OutlineWorks[id]["type"] in ("DDL","SPARK"):
        # 打印找到的第一个符合条件的作业脚本
        print(d.GetScript(projectName,id))
        break
# 注释掉的代码：读取Excel文件
# df = pandas.read_excel(workbook_path)
