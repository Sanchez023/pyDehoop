from ParamStruct import ParamOutLineWork, ParamDDLContent,ParamFlink,ParamDBInfo,ParamColumnGet,ParamSyncJob

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
        request_type: Literal["POST", "GET"],
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
            if self.request_type == "POST":
                if "json_p" not in kwargs:
                    raise ValueError("Missing 'json_p' for POST request")
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
                    url=self.url, data=json.dumps(kwargs["json_p"]), headers=headers, verify=False
                )
                return result
            else:
                raise ValueError(f"Unsupported request type: {self.request_type}")
        except Exception as e:
            logger.error(f"发送请求失败,错误信息:{e}")
            return None


class LoginModule(BaseModule):
    """登入模块类\n
    在该类下实现了与登入相关的操作
    """

    def __init__(self, base_url):
        self.url = base_url
        self.request_type = "POST"
        url = f"{base_url}/dehoop-admin/fnd/user/login?"
        super().__init__(url, self.request_type)

    def login(self, username, passwd) -> tuple[str, str,str]:
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

    def QueryProject(self, token: str, tenant_id: str) -> dict:
        """查询项目\n
        参数：
        token(str):     登入后获取的token
        tenant_id(str): 登入后获取的tenantid

        返回：
        dict_prj(dict): 项目ID与项目名称的字典
        """
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/pro/tabProjects?timestamp={timestamp}"
        self.url = url

        headers = {
            "dehooptoken": token,
            "tenantid": tenant_id,
            "connection": "keep-alive",
            "content-type": "application/json",
        }
        
        json_p = {"searchWord": "", "page": 1, "pageSize": 2147483646}

        logger.info("发送查询项目请求中...")
        
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        logger.info(f"返回状态码:  {result.status_code}")
        logger.debug(f"返回结果: \n {result.text}")

        dict_prj = {}
        try:
            project_list = json.loads(result.text)["table"]
            for i in project_list:
                logger.info(
                    f"项目名称:{i['projectName']},项目ID:{i['projectId']},环境ID:{i['envId']}"
                )
                dict_prj[i["projectName"]] = (i["projectId"], i["envId"])
            return dict_prj

        except Exception as e:
            logger.error(f"查询项目失败,错误信息:{e}")
            return None

    def QueryWorkspace(self, token: str, envid: str) -> dict:
        """查询工作空间\n
        参数：
        token(str):     登入后获取的token
        envid(str):     环境ID

        返回：
        workspaces(dict): 工作空间ID与工作空间名称的字典
        """
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/pro/env/pageQueryWorkspace?timestamp={timestamp}"
        self.url = url

        headers = {
            "dehooptoken": token,
            "connection": "keep-alive",
            "content-type": "application/json",
        }
        json_p = {
            "envId": envid,
            "searchWord": "",
            "pageSize": "99999999",
        }

        logger.info("发送查询工作空间请求中...")
        logger.debug(f"头请求内容：\n {headers}")
        logger.debug(f"请求体内容：\n {json_p}")

        result = self.send(headers, json_p=json_p)
        if result is None:
            return None
        logger.info(f"返回状态码:  {result.status_code}")
        logger.debug(f"返回结果: \n {result.text}")

        workspaces = {}
        try:
            workSpaceEnriry = json.loads(result.text)["table"][0]["workSpaceEntity"]
            name = workSpaceEnriry["name"]
            id = workSpaceEnriry["id"]
            workspaces[id] = name
            return workspaces
        except Exception as e:
            logger.error(f"查询工作空间失败,错误信息:{e}")
            return None

    
    def GetResourceType(self,token:str,projectid:str,tenantid:str)->list:
        timestamp = int(time.time())
        url = f"{self.base_url}/dehoop-admin/res/datasource/sqoopQueryType?timestamp={timestamp}"
        self.url = url
        self.request_type = "POST"
        logger.debug(self.url)
     
        json_p = {
            'timestamp':timestamp
        }
        
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
        
        result = self.send(headers,json_p = json_p)
        if result is None:
            return None
        try:
            list_dbtype = []
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            res = json.loads(result.text)["data"]
            for i in res:
                list_dbtype.append(i['type'])
            return list_dbtype
        except Exception as e:
            logger.error(f"请求失败,错误信息:{e}")
            return None   

    def GetDBResourceId(self,token:str,projectid:str,tenantid:str,param:ParamDBInfo)->dict:
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
        
        result = self.send(headers,json_p = json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            dict_db = {}
            for enty in json.loads(result.text)["data"]:
                id = enty['datasourceEntity']['id']
                name = enty['datasourceEntity']['name']
                dict_db[id] = name
            return dict_db
        except Exception as e:
            logger.error(f"请求失败,错误信息:{e}")
            return None   


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
    )->bool:
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
            if (SAVE_SUCCESS == str(json.loads(result.text)["message"])):
                return True
            return None
        except Exception as e:
            logger.error(f"保存DDL作业失败,错误信息:{e}")
            return None

    def DeleteDDLWork(
        self,token:str,projectid:str,tenantid:str,id
    )->bool:
        """删除DDL作业"""

        timestamp = int(time.time())
        url = (
            f"{self.base_url}//dehoop-admin/job/outlinework/work?timestamp={timestamp}"
        )
        self.url = url
        self.request_type = "DELETE"
        logger.debug(self.url)
        
        
        param = ParamDDLContent(workId = id)
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
        result = self.send(headers,json_p = json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            if (DELETE_SUCCESS == str(json.loads(result.text)["message"])):
                return True
            return None
        except Exception as e:
            logger.error(f"删除DDL作业失败,错误信息:{e}")
            return None   
        
    def ExecuteTestParams(self,token:str,projectid:str,tenantid:str,param):
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
        result = self.send(headers,json_p = json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            if (OPERATION_SUCCESS == str(json.loads(result.text)["message"])):
                return True
            return None
        except Exception as e:
            logger.error(f"运行测试参数作业失败,错误信息:{e}")
            return None   
    
    def ExecuteWork(self,token:str,projectid:str,tenantid:str,param:ParamOutLineWork)->str:
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
        result = self.send(headers,json_p = json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            if (EXECUTE_SUCCESS == str(json.loads(result.text)["message"])):
                excuteId = str(json.loads(result.text)["data"]["queryExecuteId"])
                return excuteId
            return None
        except Exception as e:
            logger.error(f"执行作业失败,错误信息:{e}")
            return None   
        
    def GenerateDDL(self,token:str,projectid:str,tenantid:str,param:ParamFlink)->str:
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
        
        result = self.send(headers,json_p = json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            return str(json.loads(result.text)["data"]["generateSql"])
        except Exception as e:
            logger.error(f"执行建表语句请求失败,错误信息:{e}")
            return None   
        
    def SaveOrUpdateSyncWork(self,token:str,projectid:str,tenantid:str,param:ParamSyncJob)->bool:
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
        
        result = self.send(headers,json_p = json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            return True
        except Exception as e:
            logger.error(f"保存语句请求失败,错误信息:{e}")
            return None
        
    def GetColumnInfo(self,token:str,projectid:str,tenantid:str,param:ParamColumnGet)->list:
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
        
        result = self.send(headers,json_p = json_p)
        if result is None:
            return None
        try:
            logger.info(f"返回状态码:  {result.status_code}")
            logger.debug(f"返回结果: \n {result.text}")
            return json.loads(result.text)["data"]["tableColumnInfos"]
        except Exception as e:
            logger.error(f"保存建表语句请求失败,错误信息:{e}")
            return None
            