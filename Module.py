from ParamStruct import (
    ParamOutLineWork,
    ParamDDLContent,
    ParamFlink,
    ParamDBInfo,
    ParamColumnGet,
    ParamSyncJob,
    ParamDimension,
    ParamEntitry,
    BaseStruct
)

from functools import wraps
from hashlib import md5
from log import logger
from typing import Literal
import requests
import json
import time


class BaseModule:
    """模块基类\n
    在该类主要实现了对接口的请求与返回报文的解析

    属性：
    url:          接口对应的网络地址
    request_type: 接口对应的请求方式,为枚举类型
    """

    def __init__(
        self,
        url: str,
        request_type: Literal["POST", "GET", "PUT"],
    ):
        self.url = url
        self.request_type = request_type

    def send(self, headers: dict, **kwargs) -> requests.Response:
        """发送方法\n
        该方法用于发送相应应类型的请求给指定服务
        参数：
        headers(dict): 头请求参数
        json_p(dict): 传入的请求体

        返回：
        requests.Response: 返回请求的响应对象
        """
        try: 
            if "json_p" not in kwargs:
                raise ValueError("Missing 'json_p' for POST request")
            if self.request_type == "POST":
                result = requests.post(
                    url=self.url,
                    data=json.dumps(kwargs["json_p"]),
                    headers=headers,
                    verify=False,
                )
                return result
            elif self.request_type == "GET":
                if "json_p" in kwargs:
                    result = requests.get(
                        url=self.url,
                        params=kwargs["json_p"],
                        headers=headers,
                        verify=False,
                    )
                    return result
                else:
                    try:
                        result = requests.get(url=self.url, headers=headers)
                    except Exception as e:
                        logger.error(f"发送请求失败,错误信息:{e}")
                        return None
                    return result
            elif self.request_type == "DELETE":
                result = requests.delete(
                    url=self.url,
                    data=json.dumps(kwargs["json_p"]),
                    headers=headers,
                    verify=False,
                )
                return result
            elif self.request_type == "PUT":
                result = requests.put(
                    url=self.url,
                    data=json.dumps(kwargs["json_p"]),
                    headers=headers,
                    verify=False,
                )
                return result
            else:
                raise ValueError(f"Unsupported request type: {self.request_type}")
        except Exception as e:
            logger.error(f"发送请求失败,错误信息:{e}")
            return None


def api_request(method, endpoint,descr):
    '''装饰函数\n'''
    def decorator(func):
        @wraps(func)
        def wrapper(self, token, projectid, tenantid, params:BaseStruct,response=None):
            timestamp = int(time.time())
            url = f"{self.base_url}{endpoint}?timestamp={timestamp}"
            self.url = url
            self.request_type = method
            logger.info(self.url)

            json_p = params.to_dict()

            headers = {
                "dehooptoken": token,
                "tenantid": tenantid,
                "projectid": projectid,
                "connection": "keep-alive",
                "content-type": "application/json",
            }
            logger.info("-"*25+descr+"-"*25)
            logger.info("请求发送中...")
            logger.debug(f"头请求内容：\n {headers}")
            logger.debug(f"请求体内容：\n {json_p}")

            result = self.send(headers, json_p=json_p)
            if result is None:
                return None
            try:
                logger.info(f"返回状态码:  {result.status_code}")
                logger.debug(f"返回结果: \n {result.text}")
                response = json.loads(result.text)
                return func(self, token, projectid, tenantid, params,response)
                # return json.loads(result.text)
            except Exception as e:
                logger.error(f"请求失败,错误信息:{e}")
                return None
        return wrapper
    return decorator

class LoginModule(BaseModule):
    """登入模块类\n
    在该类下实现了与登入相关的操作
    """

    def __init__(self, base_url):
        self.url = base_url
        self.request_type = "POST"
        url = f"{base_url}/dehoop-admin/fnd/user/login?"
        super().__init__(url, self.request_type)

    def login(self, username, passwd) -> tuple[str, str, str]:
        """登入方法\n
        参数：
        username(str):  登入账号
        passwd(str):    登入密码

        返回：
        tuple[str,str]: 登入成功后返回的token与tenantid
        """

        md5_passwd = md5(passwd.encode()).hexdigest()
        # 取MD5加密后的密码

        headers = {
            "content-type": "application/json",
            "connection": "keep-alive",
        }

        json_p = {
            "account": username,
            "password": md5_passwd,
            "type": "account",
        }

        logger.info("发送登入请求中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            return None

        logger.info(f"返回状态码:  {result.status_code}")
        logger.debug(f"返回结果: \n {result.text}")
        if result.status_code == 200:
            dict_res = json.loads(result.text)
            del result
            try:
                token = dict_res["data"]["token"]
                tenantid = dict_res["data"]["tenantid"]
                userId = dict_res["data"]["userinfo"]["id"]
                return token, tenantid, userId
            except KeyError:
                logger.error("未找到token或tenantid!!!")
                return None
                # raise KeyError(f"Found not param 'token' or 'tenantid',plz check the response content.")

        else:
            logger.info("登入失败,请检查账号密码！")
            return None


class PublicConfig(BaseModule):
    """公共配置类\n
    在该类下实现了对公共配置模块的部分功能"""

    def __init__(self, base_url):
        self.base_url = base_url
        self.url = base_url
        self.request_type = "POST"
        url = f"{base_url}/"
        super().__init__(url, self.request_type)

    @api_request("POST","/dehoop-admin/pro/tabProjects","查询项目接口")
    def QueryProject(self, token: str,projectid: str, tenantid: str, param:BaseStruct,response=None) -> dict:
        dict_prj = {}
        project_list = response["table"]
        for i in project_list:
            logger.info(
                f"项目名称:{i['projectName']},项目ID:{i['projectId']},环境ID:{i['envId']}"
            )
            dict_prj[i["projectName"]] = (i["projectId"], i["envId"],i["dataFieldId"])
        return dict_prj
    
    @api_request("POST","/dehoop-admin/pro/env/pageQueryWorkspace","查询工作空间接口")
    def QueryWorkspace(self, token: str, param:BaseStruct,response=None) -> dict:
       
        workspaces = {}
        workSpaceEnriry = response["table"][0]["workSpaceEntity"]
        name = workSpaceEnriry["name"]
        id = workSpaceEnriry["id"]
        workspaces[id] = name
        return workspaces
    
    @api_request("POST","/dehoop-admin/res/datasource/sqoopQueryType","获取资源类型接口")
    def GetResourceType(self, token: str, projectid: str, tenantid: str,param:BaseStruct,response=None) -> list:
        res = response["data"]
        list_dbtype = []
        for i in res:
                list_dbtype.append(i["type"])
        return list_dbtype
    
    def GetDBResourceId(
        self, token: str, projectid: str, tenantid: str, param: ParamDBInfo
    ) -> dict:
        timestamp = int(time.time())

        if bool(param.isInnerType):
            url = f"{self.base_url}/dehoop-admin/pro/env/queryInnerDataSource?timestamp={timestamp}"
        else:
            url = f"{self.base_url}/dehoop-admin/pro/env/queryDataSource?timestamp={timestamp}"
        self.url = url
        self.request_type = "POST"
        logger.debug(self.url)

        json_p = param.to_dict()

        headers = {
            "dehooptoken": token,
            "tenantid": tenantid,
            "projectid": projectid,
            "connection": "keep-alive",
            "content-type": "application/json",
        }

        logger.info("请求发送中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            dict_db = {}
            for enty in json.loads(result.text)["data"]:
                id = enty["datasourceEntity"]["id"]
                name = enty["datasourceEntity"]["name"]
                dict_db[id] = name
            return dict_db
        except Exception as e:
            logger.error(f"请求失败,错误信息:{e}")
            return None
   
    @api_request("POST","/dehoop-admin/pro/env/query","获取存储空间域接口")
    def GetSpacesInfo(self, token: str, projectid: str, tenantid: str, param:BaseStruct,response=None) -> dict:
        dict_spaces = {}
        for data in response["data"]:
            id = data["businessUnit"]["id"]
            name = data["businessUnit"]["name"]
            dict_spaces[name] = id
        return dict_spaces
    
    @api_request("POST","/dehoop-admin/dataField/queryModelingDataField","获取数据数据域接口")
    def GetDataFields(self, token: str, projectid: str, tenantid: str, param:BaseStruct,response=None) -> dict:
        dict_Field = {}
        for data in response["data"]:
            id = data["id"]
            name = data["nameCn"]
            dict_Field[name] = id
        return dict_Field

    @api_request("GET","/dehoop-admin/daq/datalayer/queryDatalayers","获取数据分层接口")
    def GetDataLayers(self, token: str, projectid: str, tenantid: str,param:BaseStruct,response=None) -> dict:
        dict_Field = {}
        for data in response["data"]:
            id = data["id"]
            shortName = data["engSimpleName"]
            name = data["name"]
            dict_Field[shortName] = {"id": id, "name": name}
        return dict_Field

    @api_request("POST","/dehoop-admin/businessProcess/queryBusinessProcess","获取业务过程接口")
    def GetBusinessProcesses(self, token: str, projectid: str, tenantid: str, param:BaseStruct,response=None):
        dict_Field = {}
      
        for data in response["table"]:
            id = data["id"]
            name = data["nameCn"]
            dict_Field[name] = id
        
        return dict_Field
    
    @api_request("GET","/dehoop-admin/modelingFieldStandard/enableStandardSearch","获取字段标准信息")
    def GetStandards(
        self,tokne:str,projectId:str,tenantid:str,params:BaseStruct,response
    ):
        return json.dumps(response["data"])
    
    
    @api_request("POST","/dehoop-admin/job/outlinework/submitWork?","提交作业")
    def SubmitJob(self, token: str, projectid: str, tenantid: str,param:BaseStruct,response=None):
        return True if response.get["message"] == "提交成功" else False
    
    @api_request("POST","sch/schedule/queryPage/submitWorks?","查询提交作业")
    def QuerySubmited(self, token: str, projectid: str, tenantid: str,param:BaseStruct,response=None):
        if response.get("message") == "查询成功":
            return [row["id"] for row in response.get["table"]]
    
    @api_request("POST","/sch/schedule/submit/works?","发布作业")
    def PublishJob(self, token: str, projectid: str, tenantid: str,param:BaseStruct,response=None):
        return  True if response.get["message"] == "提交成功" else False
    
    @api_request("POST","/dehoop-admin/package/add?","审核上传")
    def ReviewPackage(self, token: str, projectid: str, tenantid: str,param:BaseStruct,response=None):
        return True if response.get["message"] == "保存成功" else False
    
SAVE_SUCCESS = "保存成功"
DELETE_SUCCESS = "删除成功"
OPERATION_SUCCESS = "操作成功"
EXECUTE_SUCCESS = "执行成功"


class DataDevelopment(BaseModule):
    """数据开发类\n
    在该类下实现了对数据开发模块的部分功能"""

    def __init__(self, base_url):
        self.base_url = base_url
        self.url = base_url
        self.request_type = "POST"
        url = f"{base_url}/"
        super().__init__(url, self.request_type)
  
    @api_request("GET","/dehoop-admin/job/outlinework/query","查询离线作业")
    def QueryOutLineWork(
        self, token: str,  projectid: str,tenantid: str, param:BaseStruct,response=None
    ) -> tuple[dict, dict]:

      
        dict_parent = {}
        dict_works = {}
        
        def FindChildV2(child_work: list, parent: str, topid: str):
            """递归查找子节点2\n
            参数：
                child_work(list): 子节点列表
                parent(str):      父节点ID
                topid(str):       顶层节点ID
            """

            for work in child_work:
                dict_works[work["id"]] = {"name":"/".join(
                    [dict_works[parent]["name"], work["name"]]
                ),"type":work["type"]}
                dict_parent[work["id"]] = topid
                if work["child"]:
                    FindChildV2(work["child"], work["id"], topid)

        for work in response["data"]:
            dict_works[work["id"]] = {"name":work["name"],"type":work["type"]}
            if work["child"]:
                dict_works[work["id"]] = {"name":work["name"],"type":work["type"]}
                dict_parent[work["id"]] = work["id"]
                FindChildV2(work["child"], work["id"], work["id"])
        
        logger.debug(f"作业信息：{dict_works}")

        return dict_works, dict_parent
      
    @api_request("POST","/dehoop-admin/job/outlinework/work","创建DDL作业")
    def CreateDDLWork(
        self, token: str, projectid: str, tenantid: str, params: ParamOutLineWork,response=None
    ) -> str:
        id = response["data"]["id"]
        logger.info(f"返回该作业id: {id}")
        return id
     
    @api_request("GET","/dehoop-admin/job/outlinework/workScript","获取脚本内容")
    def GetScript(
        self, token: str, projectid: str, tenantid: str, params: BaseStruct,response=None
    ) -> str:
        return response["data"]["workScript"]
     
    @api_request("POST","/dehoop-admin/job/outlinework/workScript","保存DDL作业")
    def SaveOrUpdateDDLWork(
        self, token: str, projectid: str, tenantid: str, params: ParamDDLContent,response=None
    ) -> bool:
        if SAVE_SUCCESS == response["message"]:
            return True
        return None

    @api_request("DELETE","/dehoop-admin/job/outlinework/work","删除DDL作业")
    def DeleteDDLWork(self, token: str, projectid: str, tenantid: str, param:BaseStruct,response=None) -> bool:
        if DELETE_SUCCESS == response["message"]:
            return True
        return None
      
    @api_request("POST","/dehoop-admin/job/outlinework/get/executionTestParams","执行参数测试接口")
    def ExecuteTestParams(self, token: str, projectid: str, tenantid: str, param,response:None):
        if OPERATION_SUCCESS == response["message"]:
            return True
        return None

    @api_request("POST","/dehoop-admin/job/outlinework/execute","执行DDL作业接口")
    def ExecuteWork(
        self, token: str, projectid: str, tenantid: str, param: ParamOutLineWork,response:None
    ) -> str:
        if EXECUTE_SUCCESS == response["message"]:
            excuteId = response["data"]["queryExecuteId"]
            return excuteId
        return None
       
    @api_request("POST","/dehoop-admin/job/sync/create/tableSql","生成DDL语句接口")
    def GenerateDDL(
        self, token: str, projectid: str, tenantid: str, param: ParamFlink,response=None
    ) -> str:
        return response["data"]["generateSql"]
       
    @api_request("POST","/dehoop-admin/job/sync/save/syncWorkConfig","保存同步作业接口")
    def SaveOrUpdateSyncWork(
        self, token: str, projectid: str, tenantid: str, param: ParamSyncJob,response:None
    ) -> bool:
        return True

    @api_request("POST","/dehoop-admin/job/sync/tableColumnInfo","获取字段接口")
    def GetColumnInfo(
        self, token: str, projectid: str, tenantid: str, param: ParamColumnGet,response=None
    ) -> list:
        return response["data"]["tableColumnInfos"]
     
    @api_request("GET","/dehoop-admin/job/outlinework/webFlow","获取作业流ID")
    def GetWebFlow(self, token: str, projectid: str, tenantid: str, param: ParamColumnGet,response=None)->str:
        return response["data"]["id"]
    
    @api_request("POST","/dehoop-admin/job/outlinework/webFlow","保存作业流-1")
    def SaveWebFlow(self, token: str, projectid: str, tenantid: str, param: ParamColumnGet,response=None)->bool:
        return True if response["message"] == "配置成功" else False
    
    @api_request("POST","/dehoop-admin/job/outlinework/flow/saveFlowBranchLine","保存作业流-2")
    def SaveBranchLine(self, token: str, projectid: str, tenantid: str, param: ParamColumnGet,response=None)->bool:
        return True if response["message"] == "操作成功" else False
    
class ModelBuilder(BaseModule):
    """维度建模类\n
    在该类下实现了对维度建模模块的部分功能"""

    def __init__(self, base_url):
        self.base_url = base_url
        self.url = base_url
        self.request_type = "POST"
        url = f"{base_url}/"
        super().__init__(url, self.request_type)

    @api_request("POST","/dehoop-admin/modeling/saveEntityBasic","创建业务实体接口")
    def CreateEntity(
        self, token: str, projectid: str, tenantid: str, params: ParamEntitry,response
    ):
        id = response["data"]["id"]
        return id
    
    @api_request("DELETE","/dehoop-admin/modeling/deleteEntity","删除业务实体接口")
    def DeleteEntity(
        self, token: str, projectid: str, tenantid: str, params:BaseStruct,response
    ):
        if SAVE_SUCCESS == response["message"]: 
            return True
        return False
      
    @api_request("POST","/dehoop-admin/modeling/saveEntityBasic","更新业务实体接口")
    def UpdateEntity(
        self, token: str, projectid: str, tenantid: str, params: ParamEntitry,response
    ):
        id = response["data"]["id"]
        return id
  
    @api_request("POST","/dehoop-admin/modelingDataDimension/addDataDimension","创建公共维度接口")
    def CreateDimension(
        self, token: str, projectid: str, tenantid: str, params: ParamDimension,response
    ) -> str:
       
        id = response["data"]["id"]
        return id
    
    @api_request("PUT","/dehoop-admin/modelingDataDimension/updateDataDimension","更新公共维度接口")
    def UpdateDimension(
        self, token: str, projectid: str, tenantid: str, params: ParamDimension,response
    ) -> str:
        id = response["data"]["id"]
        return id

    @api_request("DELETE","/dehoop-admin/modelingDataDimension/deleteDataDimension","删除公共维度")
    def DeleteDimension(
        self, token: str, projectid: str, tenantid: str, params:BaseStruct,response
    ) -> str:
        id = response["data"]["id"]
        return id
    
    @api_request("POST","/dehoop-admin/modelingDataDimension/saveField","保存公共维度字段信息")
    def SaveDimensionField(
        self, token: str, projectid: str, tenantid: str, params:BaseStruct,response
    ) -> bool:
        if SAVE_SUCCESS == response["message"]:
            return True
        return False

    @api_request("POST","/dehoop-admin/modeling/saveEntityField?","保存业务实体字段信息")
    def SaveEntitryField(
        self, token: str, projectid: str, tenantid: str, params: ParamEntitry,response
    ):
        if SAVE_SUCCESS == response["message"]:
            return True
        return False