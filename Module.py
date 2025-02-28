from ParamStruct import (
    ParamOutLineWork,
    ParamDDLContent,
    ParamFlink,
    ParamDBInfo,
    ParamColumnGet,
    ParamSyncJob,
    ParamDimension,
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
    def decorator(func):
        @wraps(func)
        def wrapper(self, token, projectid, tenantid, params:BaseStruct,response=None):
            timestamp = int(time.time())
            url = f"{self.base_url}{endpoint}?timestamp={timestamp}"
            self.url = url
            self.request_type = method
            logger.debug(self.url)

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
            dict_prj[i["projectName"]] = (i["projectId"], i["envId"])
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

    def QueryOutLineWork(
        self, token: str, tenantid: str, projectid: str, envid: str
    ) -> tuple[dict, dict]:
        """查询数据开发作业\n
        参数：
        token(str):     登入后获取的token
        tenantid(str):  登入后获取的tenantid
        projectid(str): 项目ID
        envid(str):     环境ID

        返回：
        dict_works(dict): 作业ID与作业名称的字典
        """
        timestamp = int(time.time())
        url = (
            f"{self.base_url}/dehoop-admin/job/outlinework/query?timestamp={timestamp}"
        )
        self.url = url
        self.request_type = "GET"

        dict_works = {}
        dict_parent = {}

        def FindChildV2(child_work: list, parent: str, topid: str):
            """递归查找子节点2\n
            参数：
                child_work(list): 子节点列表
                parent(str):      父节点ID
                topid(str):       顶层节点ID
            """

            for work in child_work:
                if work["type"] == "DIR":
                    dict_works[work["id"]] = "/".join(
                        [dict_works[parent], work["name"]]
                    )
                    dict_parent[work["id"]] = topid
                if work["child"]:
                    FindChildV2(work["child"], work["id"], topid)

        headers = {
            "dehooptoken": token,
            "tenantid": tenantid,
            "projectid": projectid,
            "connection": "keep-alive",
            "content-type": "application/json",
        }

        json_p = {
            "projectId": projectid,
            "envId": envid,
        }

        logger.info("发送查询数据开发作业请求中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        logger.info(f"返回状态码:  {result.status_code}")
        logger.debug(f"返回结果: \n {result.text}")

        try:
            dict_works = {}
            for work in json.loads(result.text)["data"]:
                dict_works[work["id"]] = work["name"]

                if work["child"]:

                    dict_works[work["id"]] = work["name"]
                    dict_parent[work["id"]] = work["id"]
                    FindChildV2(work["child"], work["id"], work["id"])
            logger.debug(f"作业信息：{dict_works}")

            return dict_works, dict_parent
        except Exception as e:
            logger.error(f"查询数据开发作业失败,错误信息:{e}")
            return None

    def CreateDDLWork(
        self, token: str, projectid: str, tenantid: str, params: ParamOutLineWork
    ) -> str:
        """创建DDL工作\n
        参数：
        token(str):     登入后获取的token
        projectid(str): 项目ID
        tenantid(str):  登入后获取的tenantid
        param(ParamDDLWork): DDL作业所需参数
        """
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/job/outlinework/work?timestamp={timestamp}"
        self.url = url
        self.request_type = "POST"
        logger.debug(self.url)

        headers = {
            "dehooptoken": token,
            "tenantid": tenantid,
            "projectid": projectid,
            "connection": "keep-alive",
            "content-type": "application/json",
        }

        json_p = params.to_dict()
        logger.info("发送创建DDL工作请求中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            logger.warning(f"返回结果为空！！！")
            return None
        try:
            id = json.loads(result.text)["data"]["id"]
            logger.info(f"返回状态码:  {result.status_code}")
            logger.info(f"返回该作业id: {id}")
            logger.debug(f"返回结果: \n {result.text}")
            return id
        except Exception as e:
            logger.error(f"创建DDL作业失败,错误信息:{e}")
            return None

    def SaveOrUpdateDDLWork(
        self, token: str, projectid: str, tenantid: str, params: ParamDDLContent
    ) -> bool:
        """保存DDL作业"""
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/job/outlinework/workScript?timestamp={timestamp}"
        self.url = url
        self.request_type = "POST"
        logger.debug(self.url)

        headers = {
            "dehooptoken": token,
            "tenantid": tenantid,
            "projectid": projectid,
            "connection": "keep-alive",
            "content-type": "application/json",
        }

        json_p = params.to_dict()
        logger.info("保存更新DDL作业请求发送中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            if SAVE_SUCCESS == str(json.loads(result.text)["message"]):
                return True
            return None
        except Exception as e:
            logger.error(f"保存DDL作业失败,错误信息:{e}")
            return None

    def DeleteDDLWork(self, token: str, projectid: str, tenantid: str, id) -> bool:
        """删除DDL作业"""

        timestamp = int(time.time())
        url = (
            f"{self.base_url}//dehoop-admin/job/outlinework/work?timestamp={timestamp}"
        )
        self.url = url
        self.request_type = "DELETE"
        logger.debug(self.url)

        param = ParamDDLContent(workId=id)
        json_p = param.to_dict()

        headers = {
            "dehooptoken": token,
            "tenantid": tenantid,
            "projectid": projectid,
            "connection": "keep-alive",
            "content-type": "application/json",
        }
        logger.info("删除DDL作业请求发送中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")
        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            if DELETE_SUCCESS == str(json.loads(result.text)["message"]):
                return True
            return None
        except Exception as e:
            logger.error(f"删除DDL作业失败,错误信息:{e}")
            return None

    def ExecuteTestParams(self, token: str, projectid: str, tenantid: str, param):
        """执行参数测试作业"""
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/job/outlinework/get/executionTestParams?timestamp={timestamp}"
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

        logger.info("运行测试参数作业请求发送中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")
        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            if OPERATION_SUCCESS == str(json.loads(result.text)["message"]):
                return True
            return None
        except Exception as e:
            logger.error(f"运行测试参数作业失败,错误信息:{e}")
            return None

    def ExecuteWork(
        self, token: str, projectid: str, tenantid: str, param: ParamOutLineWork
    ) -> str:
        """执行DDL作业"""
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/job/outlinework/execute?timestamp={timestamp}"
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

        logger.info("执行作业请求发送中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")
        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            if EXECUTE_SUCCESS == str(json.loads(result.text)["message"]):
                excuteId = str(json.loads(result.text)["data"]["queryExecuteId"])
                return excuteId
            return None
        except Exception as e:
            logger.error(f"执行作业失败,错误信息:{e}")
            return None

    def GenerateDDL(
        self, token: str, projectid: str, tenantid: str, param: ParamFlink
    ) -> str:
        """生产DDL"""
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/job/sync/create/tableSql?timestamp={timestamp}"
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

        logger.info("建表语句请求发送中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            return str(json.loads(result.text)["data"]["generateSql"])
        except Exception as e:
            logger.error(f"执行建表语句请求失败,错误信息:{e}")
            return None

    def SaveOrUpdateSyncWork(
        self, token: str, projectid: str, tenantid: str, param: ParamSyncJob
    ) -> bool:
        """获取字段"""
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/job/sync/save/syncWorkConfig?timestamp={timestamp}"
        self.url = url
        self.request_type = "POST"
        logger.debug(self.url)

        json_p = param.dicts

        headers = {
            "dehooptoken": token,
            "tenantid": tenantid,
            "projectid": projectid,
            "connection": "keep-alive",
            "content-type": "application/json",
        }

        logger.info("保存语句请求发送中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            return True
        except Exception as e:
            logger.error(f"保存语句请求失败,错误信息:{e}")
            return None

    def GetColumnInfo(
        self, token: str, projectid: str, tenantid: str, param: ParamColumnGet
    ) -> list:
        """获取字段"""
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/job/sync/tableColumnInfo?timestamp={timestamp}"
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

        logger.info("保存语句请求发送中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            return json.loads(result.text)["data"]["tableColumnInfos"]
        except Exception as e:
            logger.error(f"保存建表语句请求失败,错误信息:{e}")
            return None


class ModelBuilder(BaseModule):
    """维度建模类\n
    在该类下实现了对维度建模模块的部分功能"""

    def __init__(self, base_url):
        self.base_url = base_url
        self.url = base_url
        self.request_type = "POST"
        url = f"{base_url}/"
        super().__init__(url, self.request_type)

    @api_request("POST","/dehoop-admin/job/outlinework/work","创建业务实体接口")
    def CreateEntity(
        self, token: str, projectid: str, tenantid: str, params: ParamDimension
    ):
      pass
        
    @api_request("POST","/dehoop-admin/modelingDataDimension/addDataDimension","创建公共维度接口")
    def CreateDimension(
        self, token: str, projectid: str, tenantid: str, params: ParamDimension,response
    ) -> str:
       
        id = response["data"]["id"]
        return id
    
    @api_request("PUT","/dehoop-admin/modelingDataDimension/updateDataDimension","创建公共维度接口")
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
    
    @api_request("DELETE","/dehoop-admin/modelingDataDimension/saveField","保存公共维度字段信息")
    def DeleteDimension(
        self, token: str, projectid: str, tenantid: str, params:BaseStruct,response
    ) -> bool:
        if SAVE_SUCCESS == response["message"]:
            return True
        return False

