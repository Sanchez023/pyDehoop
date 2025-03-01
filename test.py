from Dehoop import Dehoop
from Module import ParamDimension
d = Dehoop("192.168.16.100","30104")

d.Login("hbbxlb","hbbx@2024")

projectName = "API项目"
businessId = '643768242606702592'
dataFieldId='643765834929405952'

memoryId = d.GetSpacesInfo(projectName)["测试开发存储空间"]
res = d.GetBusinessProcesses(projectName,dataFieldId)
datalayerId = d.GetDataLayers(projectName)["ODS"]["id"]
pdim = ParamDimension("API生成","table_api","ATOMIC_TRANSACTIONS","None",dataFieldId,datalayerId,memoryId)
res_id = d.CreateDimension(projectName,pdim)
print(res_id)