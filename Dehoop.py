from log import logger
import socket
from Module import LoginModule, PublicConfig, DataDevelopment, ModelBuilder
from ParamStruct import (
    ParamOutLineWork,
    ParamDDLContent,
    ParamFlink,
    ParamDBInfo,
    ParamColumnGet,
    ParamSyncJob,
    ParamDimension,
    ParamEntitry,
    BaseStruct,
)
from Table import DDLStruct, GetColumns, ReplaceKeyWords_spark, ReplaceKeyWords
from utils.TransFormer import GenerateJsonFields
from typing import Literal


class Root:
    """网络根信息类\n
    用于维护对应的网络地址信息和端口信息，提供了一个端口测试的方法

    属性:
    ip:             接口对应的IP
    url:            接口对应的网络地址
    s_url:          接口对应的SSL安全协议网络地址
    resquest_url:   接口访问地址
    resquest_s_url: 接口访问SSL协议地址
    port:           接口对应的端口
    """

    def __init__(self, ip: str, port: int | str):
        self.ip = ip
        self.url = f"http://{ip}"
        self.s_url = f"https://{ip}"
        self.request_url = ":".join([self.url, str(port)])
        self.request_s_url = ":".join([self.s_url, str(port)])
        self.port = int(port)

    def test_connect(
        self,
    ) -> bool:
        """
        调用socket套接字来实现对端口的访问,返回结果为布尔型。
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:

            r = s.connect_ex((self.ip, self.port))
            s.close()
        except:
            s.close()
            return False
        if r == 0:
            return True
        else:
            return False


class Dehoop(Root):
    """得帆平台对象类\n
    在该类下以接口的方式实现了对得帆部分模块的基础操作
    """

    def __init__(self, ip, port) -> bool:
        super().__init__(ip, port)
        self.token = None
        self.tenantid = None
        self.userId = None
        self.projects = None

        self.c_workspaces = None
        self.c_prjdir = None
        self.c_nodeMatch = None
        self.c_dbsType = None
        self.c_dbids = None

        self.running_executeId = {}

    def Login(self, username: str, passwd: str):
        """执行登入\n
        在进行Login后，会获取token，租户ID，用户ID到dehoop属性中，这些属性将会在后续的方法中被调用。
        若不提前进行登入，可直接将token，tenantId，userid进行初始化赋值，若不进行初始化，后续操作会出现错误。

        参数：
        username[str]:  用户账号
        passwd[str]:    密码
        """
        result = LoginModule(self.request_url).login(username, passwd)
        if result is not None:
            self.token, self.tenantid, self.userId = result
            if self.token is not None or self.tenantid is not None:
                logger.info("登入成功")
                logger.info(f"token:{self.token}")
                logger.info(f"tenantid:{self.tenantid}")
                logger.info(f"userId:{self.userId}")
            return True
        else:
            logger.error("登入失败")
            return False

    def PreQueryProject(func):
        """查询项目装饰函数"""

        def wrapTheFunction(self, *args, **kwargs):
            if self.projects is None:
                logger.warning("未获取到项目信息，正在获取项目")
                self.QueryProject()
            return func(self, *args, **kwargs)

        return wrapTheFunction

    def QueryProject(self):
        """查询项目\n
        获取所有的项目名称以及对于的环境ID，对于的名称为ProjectName和envId。

        返回参数：
        项目ID和环境ID的字典"""

        if self.token is not None:
            p = BaseStruct(searchWord="", page="1", pageSize=2147483646)
            self.projects = PublicConfig(self.request_url).QueryProject(
                self.token, "", self.tenantid, p
            )
            if self.projects is not None:
                logger.info("查询项目成功")
                return self.projects

        else:
            logger.error("未获取到token,请先登入")
            return None

    @PreQueryProject
    def QueryWorkSpace(self, projectName: str) -> dict[str, str] | None:
        """查询工作空间\n
        获取对应项目的工作空间id

        参数：
        projectname[str]:   项目名称

        返回:
        workspaces(dict): 工作空间ID与工作空间名称的字典
        """

        logger.warning("未获取到项目信息，正在获取项目")
        self.QueryProject()
        envid: str = self.projects[projectName][1]
        p = BaseStruct(
            envid=envid,
            searchWord="",
            pageSize="99999999",
        )
        result = PublicConfig(self.request_url).QueryWorkspace(self.token, p)
        if result is not None:
            self.c_workspaces = result
            logger.info("查询工作空间成功")
            logger.info(f"工作空间信息：{self.c_workspaces}")
            return result
        else:
            logger.error("查询工作空间失败")
            return None

    @PreQueryProject
    def QueryOutLineWorks(self, projectName: str):
        """查询离线作业\n
        查询该项目下所有的作业的对应信息。
        参数:
            projectName: 项目名称
        """

        projectid: str = self.projects[projectName][0]
        envid: str = self.projects[projectName][1]
        p = BaseStruct(projectId=projectid, envId=envid)
        self.c_prjdir, self.c_nodeMatch = DataDevelopment(
            self.request_url
        ).QueryOutLineWork(self.token, projectid, self.tenantid, p)

        if self.c_prjdir is not None:
            logger.info("查询离线作业成功")
            return self.c_prjdir
            # logger.info(f"离线作业信息：{self.c_prjdir}")
        else:
            logger.error("查询离线作业失败")
            return None

    def CreateDDLWork(self, projectName: str, param: ParamOutLineWork):
        """创建DDL作业\n

        参数:
        projectName: 项目名称
        param:       DDL作业参数
        """

        if self.c_prjdir is None:
            logger.warning("未获取到项目目录信息，正在获取项目目录")
            self.QueryOutLineWorks(projectName)
        projectid: str = self.projects[projectName][0]
        param.flowId = self.c_nodeMatch[param.parentId]
        id = DataDevelopment(self.request_url).CreateDDLWork(
            self.token, projectid, self.tenantid, param
        )
        if id is not None:
            logger.info("创建DDL作业成功")
            logger.info(f"DDL作业ID:{id}")
            return id
        else:
            logger.error(f"请检查当前作业名称是否重复'{param.name}'")
            return None

    def GetScript(self, projectName: str, id: str):
        """获取脚本内容"""
        if self.c_prjdir is None:
            logger.warning("未获取到项目目录信息，正在获取项目目录")
            self.QueryOutLineWorks(projectName)
        projectid: str = self.projects[projectName][0]
        param = BaseStruct(workId=id)
        result = DataDevelopment(self.request_url).GetScript(
            self.token, projectid, self.tenantid, param
        )
        if result:
            return result
        return False

    def UpdateDDLWork(self, projectName: str, param: ParamDDLContent):
        """保存更新DDL作业

        参数：
        projectName: 项目名称
        param:       DDL内容参数
        """

        if self.c_prjdir is None:
            logger.warning("未获取到项目目录信息，正在获取项目目录")
            self.QueryOutLineWorks(projectName)
        projectid: str = self.projects[projectName][0]
        result = DataDevelopment(self.request_url).SaveOrUpdateDDLWork(
            self.token, projectid, self.tenantid, param
        )
        if result == None:
            raise Exception("创建失败")

    def DeleteWorkById(self, projectName: str, id: str):
        """删除离线作业\n
        参数：
        projectName[str]: 项目名称
        id[str]:           离线作业ID"""
        if self.c_prjdir is None:
            logger.warning("未获取到项目目录信息，正在获取项目目录")
            self.QueryOutLineWorks(projectName)
        projectid: str = self.projects[projectName][0]
        p = BaseStruct(id=id)
        res = DataDevelopment(self.request_url).DeleteDDLWork(
            self.token, projectid, self.tenantid, p
        )
        if res:
            logger.info(f"删除'{id}'作业成功！")
        else:
            logger.warning(f"删除'{id}'作业失败！")

    def ExuteDDLWork(self, projectName, param: ParamOutLineWork):
        """执行DDL作业\n

        参数：
        projectName: 项目名称
        para:         DDL作业参数
        """
        if self.c_prjdir is None:
            logger.warning("未获取到项目目录信息，正在获取项目目录")
            self.QueryOutLineWorks(projectName)
        projectid: str = self.projects[projectName][0]
        if DataDevelopment(self.request_url).ExecuteTestParams(
            self.token, projectid, self.tenantid, param
        ):
            executeid = DataDevelopment(self.request_url).ExecuteWork(
                self.token, projectid, self.tenantid, param
            )
            if executeid:
                self.running_executeId[executeid] = ""
                logger.info(f"作业:{param.id},执行成功!")
            else:
                logger.warning(f"作业：'{param.id}',执行失败！")

    def CreateDDLWorkBatch(
        self, projectName: str, params: list[ParamOutLineWork], ddls: list[DDLStruct]
    ):
        for p, d in zip(params, ddls):
            id = self.CreateDDLWork(projectName, p)
            script = d.ToScript()
            if id is not None:
                try:
                    p2 = ParamDDLContent(id=id, workScript=script)
                    self.UpdateDDLWork(projectName, p2)
                    self.ExecuteDDLWork(projectName, p2)
                except:
                    self.DeleteWorkById(projectName, id)
                    continue

    def CreateOutlineWorkBatch(
        self,
        projectName: str,
        parentid: str,
        workspaceId,
        df: list,
        type: Literal["ODS", "STG", "SPARK", "DDL"],
        fromdb: str,
        todb: str = None,
    ):
        "批量创建作业"

        for i in range(len(df)):
            name = df.iloc[i, 0]
            descr = df.iloc[i, 1]

            TABLENAME = name
            DESCR = descr
            script = self.GenerateDDLScript(
                projectName, fromdb, todb, "HBCORE." + TABLENAME
            )
            if script is None:
                continue

            match type:
                case "SPARK":
                    INPARAM = TABLENAME
                    p = ParamOutLineWork(
                        parentId=parentid,
                        name="ODS_HBCORE_" + INPARAM + "_ONYARN",
                        descr=DESCR,
                        workspaceId=workspaceId,
                        type="SparkSQL",
                        director=self.tenantid,
                    )
                case "ODS":
                    INPARAM = "ODS_HBCORE_" + TABLENAME
                    p = ParamOutLineWork(
                        parentId=parentid,
                        name=INPARAM,
                        descr=DESCR,
                        workspaceId=workspaceId,
                        director=self.tenantid,
                    )
                case "STG":
                    INPARAM = "STG_HBCORE_" + TABLENAME
                    p = ParamOutLineWork(
                        parentId=parentid,
                        name=INPARAM,
                        descr=DESCR,
                        workspaceId=workspaceId,
                        director=self.tenantid,
                    )

            id = self.CreateDDLWork(projectName, p)

            if type == "spark":
                script = ReplaceKeyWords_spark(INPARAM)
            elif type == "STG":
                script = ReplaceKeyWords(INPARAM, script, DESCR, False)
            else:
                script = ReplaceKeyWords(INPARAM, script, DESCR, True)

            if id is not None:

                try:
                    p2 = ParamDDLContent(id=id, workScript=script)
                    self.UpdateDDLWork(projectName, p2)
                    if type != "spark":
                        self.ExecuteDDLWork(projectName, p2)
                except:
                    self.DeleteWorkById(projectName, id)

    def GenerateDDLScript(
        self, projectName: str, fromdb: str, todb: str, tableName: str
    ) -> str:
        """生成DDL建表语句\n
        该方法会获取建表语句所生成的所有字段信息，并以文本的方式返回。
        参数：
        projectNmae[str]: 项目名称
        fromDb[str]:      来源数据库ID
        toDb[str]:        去向数据库ID
        tableName[str]:   数据表名称
        """
        if self.c_prjdir is None:
            logger.warning("未获取到项目目录信息，正在获取项目目录")
            self.QueryOutLineWorks(projectName)
        projectid: str = self.projects[projectName][0]
        param = ParamFlink(fromDbId=fromdb, toDbId=todb, tableName=tableName)
        res = DataDevelopment(self.request_url).GenerateDDL(
            self.token, projectid, self.tenantid, param
        )
        if res is None:
            return None
        return GetColumns(res)

    def GetResourceType(self, projectName):
        import time

        """获取数据库类型\n
        该方法会获取对应项目下的所有的数据库类型

        参数：
        projectName[str]:   项目名称"""
        if self.c_prjdir is None:
            logger.warning("未获取到项目目录信息，正在获取项目目录")
            self.QueryOutLineWorks(projectName)
        projectid: str = self.projects[projectName][0]
        timestamp = int(time.time())
        p = BaseStruct(timestamp=timestamp)
        res = PublicConfig(self.request_url).GetResourceType(
            self.token, projectid, self.tenantid, p
        )
        if res is not None:
            self.c_dbsType = res
            return res
        else:
            return None

    def GetDBResourceId(self, projectName, type: str, isInnertype: bool = False):
        """获取数据来源ID\n
        获取该项目下指定类型的数据库类型的ID。

        参数：
        projectName[str]:   项目名称
        type[str]:          对应的数据库类型
        isInnertype[bool]:  是否为内部数据源：为True时，表示为获取去向源的数据库，为Flase时，表示为获取来源的数据库
        """
        if self.c_prjdir is None:
            logger.warning("未获取到项目目录信息，正在获取项目目录")
            self.QueryOutLineWorks(projectName)
        projectid: str = self.projects[projectName][0]
        envId = self.projects[projectName][1]

        param = ParamDBInfo(envId=envId, type=type, isInnerType=isInnertype)
        res = PublicConfig(self.request_url).GetDBResourceId(
            self.token, projectid, self.tenantid, param
        )
        if res is not None:
            self.c_dbids = res
            return res
        else:
            return None

    @PreQueryProject
    def GetColumnInfos(
        self,
        projectName: str,
        resourceId: str,
        tableName: str,
        type: Literal["src", "dist"],
    ) -> list:
        """获字段信息\n
        获取对应数据表的字段信息

        参数:
        projectName[str]:   项目名称
        resourceId[str]:    数据库ID
        tableName[str]:     对应数据表名称
        type[str]:          类型（来源数据库src或去向数据库dist）
        """

        projectid: str = self.projects[projectName][0]
        param = ParamColumnGet(dbSourceId=resourceId, tableName=tableName, type=type)

        res = DataDevelopment(self.request_url).GetColumnInfo(
            self.token, projectid, self.tenantid, param
        )
        if res is not None:
            return res
        else:
            return None

    @PreQueryProject
    def SaveOrUpdateSyncWork(
        self,
        projectName: str,
        id: str,
        fromDbId: str,
        fromTableName: str,
        toDbId: str,
        toTableName: str,
        mappingList: list,
        addColumn: list,
        maxConCurrentNum: int = 1,
        maxTransSpeed: int = 10,
    ):
        """保存更新离线同步作业\n

        projectName[str]:       项目名称
        id[str]:                作业ID
        fromDbId[str]:          来源数据库Id
        fromTableName[str]:     来源表名称
        toDbId[str]:            去向数据库Id
        toTableName[str]:       去向数据表名称
        mappingList[List]:      匹配字段列表
        addColumn[list]:        来源字段列表
        maxConCurrentNum[int]:  最大并发数
        maxTransSpeed[int]:     最大传输速度
        """
        projectid: str = self.projects[projectName][0]
        param = ParamSyncJob(
            id=id,
            fromDbId=fromDbId,
            fromTableName=fromTableName,
            toDbId=toDbId,
            toTableName=toTableName,
            addColumn=addColumn,
            mappingList=mappingList,
            maxConCurrentNum=maxConCurrentNum,
            maxTransSpeed=maxTransSpeed,
        )

        res = DataDevelopment(self.request_url).SaveOrUpdateSyncWork(
            self.token, projectid, self.tenantid, param
        )
        if res is not None:
            return res
        else:
            return None

    @PreQueryProject
    def GetSpacesInfo(self, projectName: str) -> dict[str, str]:
        projectid: str = self.projects[projectName][0]

        p = BaseStruct(projectId=projectid)
        res = PublicConfig(self.request_url).GetSpacesInfo(
            self.token, projectid, self.tenantid, p
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到储存空间信息")
            return None

    @PreQueryProject
    def GetDataFields(self, projectName: str) -> dict[str, str]:
        projectid: str = self.projects[projectName][0]
        p = BaseStruct()
        res = PublicConfig(self.request_url).GetDataFields(
            self.token, projectid, self.tenantid, p
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到数据域信息")
            return None

    @PreQueryProject
    def GetDataLayers(self, projectName: str) -> dict[str, str]:
        projectid: str = self.projects[projectName][0]
        p = BaseStruct(projectId=projectid)
        res = PublicConfig(self.request_url).GetDataLayers(
            self.token, projectid, self.tenantid, p
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到数据分层信息")
            return None

    @PreQueryProject
    def GetBusinessProcesses(self, projectName: str) -> dict[str, str]:
        projectid: str = self.projects[projectName][0]
        dataFieldId: str = self.projects[projectName][2]
        param = BaseStruct(dataField=dataFieldId)
        res = PublicConfig(self.request_url).GetBusinessProcesses(
            self.token, projectid, self.tenantid, param
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到业务过程信息")
            return None

    @PreQueryProject
    def CreateDimension(self, projectName: str, params: ParamDimension):
        projectid: str = self.projects[projectName][0]
        dataFieldId: str = self.projects[projectName][2]
        params.projectId = projectid
        params.dataFieldId = dataFieldId
        res = ModelBuilder(self.request_url).CreateDimension(
            self.token, projectid, self.tenantid, params
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到业务过程信息")
            return None

    @PreQueryProject
    def UpdateDimension(self, projectName: str, params: ParamDimension):
        projectid: str = self.projects[projectName][0]
        dataFieldId: str = self.projects[projectName][2]
        params.projectId = projectid
        params.dataFieldId = dataFieldId
        res = ModelBuilder(self.request_url).UpdateDimension(
            self.token, projectid, self.tenantid, params
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到业务过程信息")
            return None

    @PreQueryProject
    def DeleteDimension(self, projectName: str, id: str):
        projectid: str = self.projects[projectName][0]
        params = BaseStruct(id=id)
        res = ModelBuilder(self.request_url).DeleteDimension(
            self.token, projectid, self.tenantid, params
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到业务过程信息")
            return None

    @PreQueryProject
    def DownloadStandars(self, projectName: str):
        projectid: str = self.projects[projectName][0]
        p = BaseStruct()
        res = PublicConfig(self.request_url).GetStandards(
            self.token, projectid, self.tenantid, p
        )

        if res is not None:
            with open("./standarsInfo/data.json", "w", encoding="utf-8") as f:
                f.write(res)
            return True
        else:
            logger.warning("为获取的标准字段信息")
            return None

    @PreQueryProject
    def SaveDimensionFields(self, projectName: str, id: str, field: list):
        projectid: str = self.projects[projectName][0]
        fields = GenerateJsonFields(field)
        p = BaseStruct(id=id, fields=fields)

        res = ModelBuilder(self.request_url).SaveDimensionField(
            self.token, projectid, self.tenantid, p
        )
        if res is not None:
            return res
        else:
            logger.warning("未能保存公共维度字段程信息")
            return None

    @PreQueryProject
    def CreateEntity(self, projectName: str, params: ParamEntitry):
        projectid: str = self.projects[projectName][0]
        dataFieldId: str = self.projects[projectName][2]
        params.projectId = projectid
        params.dataFieldId = dataFieldId
        res = ModelBuilder(self.request_url).CreateEntity(
            self.token, projectid, self.tenantid, params
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到业务实体信息")
            return None

    @PreQueryProject
    def UpdateEntity(self, projectName: str, params: ParamEntitry):
        projectid: str = self.projects[projectName][0]
        dataFieldId: str = self.projects[projectName][2]
        params.projectId = projectid
        params.dataFieldId = dataFieldId
        res = ModelBuilder(self.request_url).UpdateEntity(
            self.token, projectid, self.tenantid, params
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到业务实体信息")
            return None

    @PreQueryProject
    def DeleteEntity(self, projectName: str, id: str):
        projectid: str = self.projects[projectName][0]
        params = BaseStruct(id=id)
        res = ModelBuilder(self.request_url).DeleteEntity(
            self.token, projectid, self.tenantid, params
        )
        if res is not None:
            return res
        else:
            logger.warning("未获取到业务实体信息")
            return None

    @PreQueryProject
    def SaveEntitryFields(self, projectName: str, id: str, field: list):
        projectid: str = self.projects[projectName][0]
        fields = GenerateJsonFields(field)
        p = BaseStruct(id=id, fields=fields)

        res = ModelBuilder(self.request_url).SaveEntitryField(
            self.token, projectid, self.tenantid, p
        )
        if res is not None:
            return res
        else:
            logger.warning("未能保存公共维度字段程信息")
            return None

    @PreQueryProject
    def PublishReview(self, projectName: str, flowid: str, desc: str = None):
        projectid: str = self.projects[projectName][0]
        print(self.c_nodeMatch is None)

        ids = [k for k, v in self.c_nodeMatch.items() if v == flowid]
     

        index = 0
        for n in range(0, len(ids) // 10):
            for id in ids[index : index + 10]:
                p = BaseStruct(id=id)
                PublicConfig(self.request_url).SubmitJob(
                    self.token, projectid, self.tenantid, p
                )
            new_ids: list = PublicConfig(self.request_url).QuerySubmited(
                self.token, projectid, self.tenantid, p
            )
            p = BaseStruct(
                envId=self.projects[projectName][1],
                flowId=flowid,
                page=1,
                pageSize=20,
            )
            p = BaseStruct(idList=new_ids, comment="")
            PublicConfig(self.request_url).PublishJob(
                self.token, projectid, self.tenantid, p
            )
            index += 10
        for id in ids[index:]:
            p = BaseStruct(id=id)
            PublicConfig(self.request_url).SubmitJob(
                self.token, projectid, self.tenantid, p
            )
        p = BaseStruct(
            envId=self.projects[projectName][1], flowId=flowid, page=1, pageSize=20
        )
        new_ids: list = PublicConfig(self.request_url).QuerySubmited(
            self.token, projectid, self.tenantid, p
        )
        p = BaseStruct(idList=new_ids, comment="")
        PublicConfig(self.request_url).PublishJob(
            self.token, projectid, self.tenantid, p
        )

    # if ids:
    #     p = BaseStruct()
    #     if PublicConfig(self.request_url).SubmitJob(
    #         self.token, projectid, self.tenantid, p
    #     ):
    #         p = BaseStruct()
    #         if PublicConfig(self.request_url).PublishJob(
    #             self.token, projectid, self.tenantid, p
    #         ):
    #             if PublicConfig(self.request_url).ReviewPackage(
    #                 self.token, projectid, self.tenantid, p
    #             ):
    #                 return True
    #     return False

    @PreQueryProject
    def ClearFlowBranch(self, projectName: str, flowid: str):
        projectid: str = self.projects[projectName][0]
        p = BaseStruct(flowId=flowid)
        id = DataDevelopment(self.request_url).GetWebFlow(
            self.token, projectid, self.tenantid, p
        )
        p = BaseStruct(
            flowWebConfig=str(
                [
                    {
                        "id": "START",
                        "args": {"x": 50, "y": 274},
                        "name": "start",
                        "type": "flow",
                    }
                ]
            ),
            nodeIdList=["START"],
            flowId=flowid,
            flowRowConfig=[],
            id=id,
        )
        res = DataDevelopment(self.request_url).SaveWebFlow(
            self.token, projectid, self.tenantid, p
        )
        if res:
            p = BaseStruct(branchLines=[], workid=id)
            return DataDevelopment(self.request_url).SaveBranchLine(
                self.token, projectid, self.tenantid, p
            )

    @PreQueryProject
    def PublishWorkBatch(
        self, projectName: str, workIds: list[str] = None, desc: str = "批量发布"
    ):
        """批量发布作业

        参数:
            projectName: 项目名称
            workIds: 要发布的作业ID列表，如果为None则发布所有已提交的作业
            desc: 发布描述

        返回:
            bool: 是否全部发布成功
        """
        projectid: str = self.projects[projectName][0]

        # 如果没有提供作业ID列表，则查询所有已提交的作业
        if workIds is None:
            p = BaseStruct()
            workIds = PublicConfig(self.request_url).QuerySubmited(
                self.token, projectid, self.tenantid, p
            )

        if not workIds:
            logger.warning("没有找到需要发布的作业")
            return False

        success_count = 0
        total_count = len(workIds)

        for work_id in workIds:
            p = BaseStruct(id=work_id)
            # 提交作业
            if PublicConfig(self.request_url).SubmitJob(
                self.token, projectid, self.tenantid, p
            ):
                # 发布作业
                if PublicConfig(self.request_url).PublishJob(
                    self.token, projectid, self.tenantid, p
                ):
                    # 审核作业
                    if PublicConfig(self.request_url).ReviewPackage(
                        self.token, projectid, self.tenantid, p
                    ):
                        success_count += 1
                        logger.info(f"作业 {work_id} 发布成功")
                    else:
                        logger.error(f"作业 {work_id} 审核失败")
                else:
                    logger.error(f"作业 {work_id} 发布失败")
            else:
                logger.error(f"作业 {work_id} 提交失败")

        logger.info(f"批量发布完成，成功 {success_count}/{total_count}")
        return success_count == total_count
