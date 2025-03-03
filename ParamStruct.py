import json
from uuid import uuid4

from typing import Literal


class BaseStruct:
    """参数结构体\n
    该类用于定义接口请求的参数结构体
    """

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)

    def to_dict(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self.__dict__)


class ParamOutLineWork(BaseStruct):
    """DDL工作参数结构体\n
    该类用于定义DDL工作的参数结构体\n

    参数:
        parentId:   父作业ID
        name:       作业名称
        type:       作业类型
        flowId:     流程ID
        director:   负责人
        descr:      描述
        workspaceId:工作空间ID
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.id = kwargs.get("id", "")
        self.workId = kwargs.get("workId", "")
        self.parentId = kwargs.get("parentId")
        self.name = kwargs.get("name")
        self.flowId = kwargs.get("flowId")
        self.type = kwargs.get("type", "DDL")
        self.director = kwargs.get("director")
        self.descr = kwargs.get("descr")
        self.workspaceId = kwargs.get("workspaceId")


class ParamDDLContent(BaseStruct):
    """DDl内容参数结构体\n
    该类用于存放DDL语句等内容

    参数:
    id:         作业id,
    excuteId:   执行id
    workScript: SQL语句
    keyWords:   传入SQL的关键字,
    remain:     false
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        default_keywords = [
            "DROP",
            "TABLE",
            "IF",
            "CREATE",
            "LANGUAGE",
            "TIMESTAMP",
            "ROW",
            "BY",
            "AS",
        ]
        self.id = kwargs.get("id")
        self.workId = kwargs.get("workId", self.id)
        self.executeId = kwargs.get("executeId", uuid4().hex)
        self.workScript = kwargs.get("workScript")
        self.keywords = kwargs.get("keyWords", default_keywords)
        self.remain = kwargs.get("remain", False)
        self.script = self.workScript


class ParamFlink(BaseStruct):
    """
        {
        "fromDbId": "643763299611049984",
        "toDbId": "643766294172139520",
        "schema": None,
        "tableName": "GCMAXDEALERCODE"
    }"""

    def __init__(self, fromDbId: str, toDbId: str, tableName: str, **kwargs):
        super().__init__(**kwargs)

        self.fromDbId = fromDbId
        self.toDbId = toDbId
        self.tableName = tableName
        self.schema = None


class ParamDBInfo(BaseStruct):
    def __init__(
        self, envId: str, type: bool = False, isInnerType: bool = False, **kwargs
    ):
        super().__init__(**kwargs)

        self.envId = envId
        self.type = type
        self.isInnerType = isInnerType


class ParamColumnGet(BaseStruct):
    def __init__(
        self, dbSourceId: str, tableName: str, type: Literal["dist", "src"], **kwargs
    ):
        super().__init__(**kwargs)

        self.dbSourceId = dbSourceId
        self.tableName = tableName
        self.dbSourceDirection = type
        self.schema = kwargs.get("schema")
        self.dataListPath = kwargs.get("dataListPath")


class ParamSyncJob(BaseStruct):
    def __init__(
        self,
        id: str,
        fromDbId: str,
        fromTableName: str,
        toDbId: str,
        toTableName: str,
        mappingList: list,
        addColumn: list,
        maxConCurrentNum: int = 1,
        maxTransSpeed: int = 10,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.to_dict = self.dicts
        
        self.dicts = {
            "fromDbId": fromDbId,
            "fromDbTable": fromTableName,
            "toDbId": toDbId,
            "toDbTable": toTableName,
            "workId": id,
            "conditionSql": kwargs.get("conditionsql"),
            "partitionFiltering": "",
            "maxConcurrentNum": maxConCurrentNum,
            "maxTransferSpeed": maxTransSpeed,
            "maxErrorCount": None,
            "appendMode": "overwrite",
            "filterSql": "",
            "splitByValue": "",
            "timestampField": None,
            "partitionValueList": None,
            "columnGroup": None,
            "linkTables": [],
            "mappingList": mappingList,
            "apiConfigId": None,
            "remain": False,
            "postSql": None,
            "preSql": None,
            "addColumns": addColumn,
            "readMode": "",
            "rowkey": "",
            "nullMode": "",
            "walFlag": None,
            "dirType": "",
            "dirPath": [],
            "fromLineDelimiter": "\\n",
            "fromFieldDelimiter": ",",
            "fromEncode": "UTF-8",
            "fromNullValue": "null",
            "compressFormat": "",
            "withTableHeader": False,
            "jsonType": "",
            "recordNode": "",
            "filePath": "",
            "fileType": "",
            "toLineDelimiter": "\\n",
            "toFieldDelimiter": ",",
            "toEncode": "UTF-8",
            "toNullValue": "null",
            "fileConflict": "truncate",
            "redisKeys": "",
            "dataType": "hash",
            "writeMode": "hset",
            "keyDelimiter": "\\u0001",
            "cacheExpirationTime": "",
            "key": [],
            "replaceKey": "",
            "fromDataListPath": None,
            "toDataListPath": None,
        }


class ParamDimension(BaseStruct):
    def __init__(
        self,
        name: str,
        tableName: str,
        granularity: Literal["ATOMIC_TRANSACTIONS", "PERIODIC_SNAPSHOT"],
        descr: str,
        dataLayerId: str,
        memorySpaceId: str,
        dataFieldId: str=None,
        projectId: str=None,
        type: str = "GENERAL",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.name = name
        self.tableName = tableName
        self.granularity = granularity
        self.descr = descr
        self.dataFieldId = dataFieldId
        self.dataLayerId = dataLayerId
        self.type = type
        self.memorySpaceId = memorySpaceId
        self.projectId = projectId
        self.id  = kwargs.get("id",None)
        
class ParamEntitry(BaseStruct):
    def __init__(
        self,
        name: str,
        tableName: str,
        granularity: Literal["ATOMIC_TRANSACTIONS", "PERIODIC_SNAPSHOT"],
        descr: str,
        dataLayerId: str,
        memorySpaceId: str,
        dataFieldId: str=None,
        projectId: str=None,
        type: str = "GENERAL",
        businessType:str=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.name = name
        self.tableName = tableName
        self.granularity = granularity
        self.descr = descr
        self.dataFieldId = dataFieldId
        self.dataLayerId = dataLayerId
        self.type = type
        self.memorySpaceId = memorySpaceId
        self.projectId = projectId
        self.id  = kwargs.get("id","")
        self.businessProcessId = businessType

