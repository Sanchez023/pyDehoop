from Dehoop import Dehoop
from ParamStruct import ParamOutLineWork, ParamDDLContent
import pandas as pd

if __name__ == '__main__':
    # 初始化Dehoop对象
    d = Dehoop("10.1.8.17", 30104)
    
    # 登录
    d.Login("username", "password")
    
    # 项目名称
    projectName = "中台-客户管理其他域模型开发"
    
    # 查询离线作业
    works = d.QueryOutLineWorks(projectName)
    
    # 筛选需要发布的作业ID
    work_ids = []
    for id, info in works.items():
        # 这里可以根据需要筛选特定类型或名称的作业
        if info["type"] in ("DDL", "SYNC"):
            work_ids.append(id)
    
    # 批量发布作业
    result = d.PublishWorkBatch(projectName, work_ids, "批量发布DDL和同步作业")
    
    if result:
        print("所有作业发布成功")
    else:
        print("部分作业发布失败，请查看日志")