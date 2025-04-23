from Dehoop import Dehoop
import pandas 
from pandas import DataFrame
from ParamStruct import ParamEntitry,ParamDimension

# 示例脚本：演示如何创建实体和维度模型

# 注释掉的连接配置
# d = Dehoop("192.168.16.100","30104")
# 初始化Dehoop对象，连接到指定IP和端口
d = Dehoop("10.1.8.17","30104")

# 登录系统
d.Login("liub","hbbx@2024SxdC")

# 注释掉的token设置方式
# d.token="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJkZWhvb3B1c2VyaWQiOiI2NTcyNTM5NTk3NjQ4MDM1ODQiLCJ0ZW5hbnRpZCI6IjY1NjYwOTExODM5OTc1ODMzNiIsImV4cCI6MTc0MTI1MzI0NSwiaWF0IjoxNzQwOTk0MDQ1fQ.SnfX9ygULzs-VEY0-iYtNnAJgy-92alKh0R0OF2s1cifamwPU1eSS8VAa86CBJUbuI0YKPaIm3H7x3nkCqVt8Q"
# d.tenantid = "656609118399758336"
# 设置项目名称
projectName = "财务域模型开发"
# 注释掉的下载标准方法
# d.DownloadStandars(projectName)
# 读取Excel文件中的所有工作表，不使用表头
dfs = pandas.read_excel("C:/Xiaomi Cloud/Desktop/DWE.xlsm",sheet_name=None,header=None)


# 获取数据层ID（CDM层）
dataLayerId = d.GetDataLayers(projectName)["CDM"]["id"]
# 获取内存空间ID
memorySpaceId = d.GetSpacesInfo(projectName)['核心开发存储空间']
# 获取业务流程ID
busniessType = d.GetBusinessProcesses(projectName)["资金接口"]

# 遍历所有工作表
for sheetName in dfs:
    # 跳过字段标准工作表
    if sheetName  ==  "字段标准":
        continue
    # 获取当前工作表数据
    df:DataFrame= dfs[sheetName]
    # 设置列名为数字索引
    df.columns = [i for i in range(0,len(df.columns))]
    # 获取列名（第4行数据），空值填充为"remark"
    colums = df.iloc[3].fillna("remark").tolist()
    # 获取表中文名（第1行第3列）
    tableCName = df.iloc[0,2]
    # 获取表英文名（第2行第3列）
    tableName = df.iloc[1,2]
    # 获取表描述（第3行第3列）
    desc = df.iloc[2,2]

    # 提取数据部分（第5行及以后）
    data_frame = df.iloc[4:,:].reset_index(drop=True)
    # 设置列名
    data_frame.columns= colums
    # 初始化字段字典
    fields = {}
    # 选择需要的列并填充空值
    data_frame = data_frame[["标准字段分类","源表字段中文名","标准物理字段","主键","外键","remark"]].fillna("")
    # 筛选remark为0的行
    data_frame = data_frame[data_frame["remark"] == 0]
    
    # 创建实体参数对象
    p = ParamEntitry(tableCName,tableName,"ATOMIC_TRANSACTIONS",desc,dataLayerId,memorySpaceId,businessType=busniessType)
    # 创建实体
    id = d.CreateEntity(projectName,p)
    
    # 注释掉的创建维度代码
    # p = ParamDimension(tableCName,tableName,"ATOMIC_TRANSACTIONS",desc,dataLayerId,memorySpaceId,businessType=busniessType)
    # id = d.CreateDimension(projectName,p)

    # 如果创建成功，继续处理字段
    if id:
        
        try:
            # 遍历数据框中的每一行
            for i in data_frame.values:
                # 判断是否为主键
                isPK = False if i[3] == '' else True
                # 判断是否为外键
                isFK = False if i[4] == '' else True
                # 将字段信息添加到字段字典中
                fields[i[2]] = {"clsName":i[0],"cname":i[1],"isPK":isPK,"isFK":isFK}
            
            # 打印字段信息
            print(fields)
            # 注释掉的保存维度字段方法
            # d.SaveDimensionFields(projectName,id,fields)
            # 保存实体字段
            d.SaveEntitryFields(projectName,id,fields)
        except Exception as e:
            # 发生异常时打印错误信息
            print(e)
            print(sheetName)
            # 删除创建的实体
            d.DeleteEntity(projectName,id)
            # 注释掉的删除维度方法
            # d.DeleteDimension(projectName,id)
