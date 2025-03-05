from uuid import uuid4


def generateUUID(isHyphen: bool = True):
    '''生成UUID，若isHyphen为TRUE则会生成横杠用于间隔，若为False则会生成去除横杠的UUID'''
    if isHyphen:
        return uuid4().__str__()
    return uuid4().hex


def Transerfrom_addColumn(origin_list: list[dict], tableName):
    '''用于在原字段JOSN后后添加UUID参数等信息'''
    addColumns = []
    for dict_column in origin_list:
        dict_column["table"] = tableName
        dict_column["$__seq_ID"] = "__seq_ID_" + generateUUID()
        dict_column["addColumn"] = None
        addColumns.append(dict_column)

    return addColumns


def Transerfrom_mappingList(origin_list: list[dict], fromTable: str, toTableName: str):
    '''用于在同步作业中匹配字段'''
    mappingList = []
    for column in origin_list:

        mapping_dict = {}

        mapping_dict["from"] = str(column["field"]).upper()
        mapping_dict["fromApiFieldId"] = fromTable
        mapping_dict["to"] = column["field"]
        mapping_dict["fromTable"] = fromTable
        mapping_dict["toTable"] = toTableName
        mappingList.append(mapping_dict)

    return mappingList


def ExtraColumn(field: str, table: str, func: str):
    '''用于生成函数字段，func参数对应中台的函数文本'''
    uuid = generateUUID(False)
    dict_extraColumn = {
        "addColumn": True,
        "field": uuid,
        "showField": "-",
        "fieldtype": "-",
        "type": "-",
        "table": table,
        "$__seq_ID": "__seq_ID_" + generateUUID(),
        "fromTransform": func,
    }
    return field, uuid, dict_extraColumn


def ReMappingList(origin_list: list[dict], field: str, uuid: str):
    '''重新匹配字段列表'''
    temp = -1
    for index, map in enumerate(origin_list):
        if map["to"] == field:
            temp = index
    origin_list[temp]["from"] = uuid
    return origin_list


def GetFieldInfosFromJS() -> dict:
    '''用于从本地目录下读取数据标准的数据，需要提前下载成json格式'''
    import json

    with open("./standarsInfo/data.json", "r", encoding="utf-8") as f:
        list_field = json.load(f)
    dict_field = {}
    for c in list_field:
        name = c["name"]
        value = c["children"]
        dict_value = {}
        for v in value:
            son_name = v["code"]
            v["pathName"] = "/" + name + "/" + v["name"]
            dict_value[son_name] = v
        dict_field[name] = dict_value
    return dict_field


def GenerateFieldJsonParam(**kwargs):
    '''将读取到的单个文本字段转化为JSON形式的参数'''
    name = kwargs.get("name")
    id = kwargs.get("id")
    code = kwargs.get("code")
    fieldType = kwargs.get("fieldType")
    precision = kwargs.get("precision")
    scale = kwargs.get("scale")
    comment = kwargs.get("comment")
    pathName = kwargs.get("pathName")
    modelRelationship = kwargs.get("modelRelationship")
    isPrimaryKey = kwargs.get("isPrimaryKey", False)
    seqNo = kwargs.get("No")

    fieldInfo = {
        "moduleType": "DOUBLE",
        "label": "小数",
        "icon": "double-icon",
        "desc": "VARCHAR",
        "fieldType": fieldType,
        "precision": precision,
        "scale": scale,
        "maxLimit": 32,
        "maxDecimal": 10,
        "configs": [
            "CompConfigInfo",
            "CompConfigFieldType",
            "CompConfigFieldName",
            "CompConfigLogicName",
            "CompConfigDecimalSetting",
            "CompConfigFieldDescr",
            "CompConfigPrimaryKey",
            "CompConfigModelRelationship",
        ],
        "id": generateUUID(False),
        "fieldCreated": False,
        "name": code,
        "type": fieldType,
        "comment": comment,
        "sequenceNumber": seqNo,
        "isPrimaryKey": isPrimaryKey,
        "isNotNull": False,
        "isSys": None,
        "isPartitionField": None,
        "fieldName": name,
        "logicName": code,
        "entityId": None,
        "modelRelationship": modelRelationship,
        "isIndependent": None,
        "initialization": None,
        "isAssociated": None,
        "measureUnitId": None,
        "fieldStandardId": id,
        "fieldStandardName": pathName,
        "lengthLimit": None,
        "active": False,
        "$__seq_ID": "__seq_ID_" + generateUUID(),
    }
    return fieldInfo


def GenerateJsonFields(fields: dict[str, dict]):
    '''将所有的字段已JSON格式进行整合成一个集合'''
    defaultField = {
        "moduleType": "NAME",
        "label": "名称",
        "icon": "name-icon",
        "desc": "VARCHAR",
        "fieldType": "STRING",
        "lengthLimit": 128,
        "maxLimit": 128,
        "configs": [
            "CompConfigInfo",
            "CompConfigFieldType",
            "CompConfigFieldName",
            "CompConfigLogicName",
            "CompConfigLengthLimit",
            "CompConfigFieldDescr",
            "CompConfigPrimaryKey",
            "CompConfigModelRelationship",
        ],
        "id": generateUUID(),
        "fieldCreated": False,
        "name": "DS",
        "type": "STRING",
        "precision": None,
        "scale": None,
        "comment": "分区字段",
        "sequenceNumber": 4,
        "isPrimaryKey": False,
        "isNotNull": False,
        "isSys": True,
        "isPartitionField": True,
        "fieldName": "业务日期，yyyymmdd",
        "logicName": "DS",
        "entityId": None,
        "modelRelationship": "ATTRIBUTE",
        "isIndependent": None,
        "initialization": None,
        "isAssociated": None,
        "measureUnitId": None,
        "fieldStandardId": None,
        "fieldStandardName": None,
        "active": False,
        "$__seq_ID": "__seq_ID_797c560b-3d3e-a142-8471-b5a2aadd600a",
        "selected": False,
    }
    list_fields = [defaultField]
    dict_fields = GetFieldInfosFromJS()
    No = 1
    pk_flag = False
    for code in fields.keys():
        fieldInfo: dict = dict_fields[fields[code]["clsName"]][code]
        name = fields[code]["cname"]
        id = fieldInfo["id"]
        fieldType = fieldInfo["dataType"]
        precision = fieldInfo["fieldLengthValue"]
        scale = fieldInfo["fieldPrecision"]
        comment = fieldInfo["descr"]
        pathName = fieldInfo["pathName"]

        if fields[code]["isFK"] or fields[code]["isPK"]:
            modelRelationship = "RELATION_FIELD"
        elif fieldType == "STRING":
            modelRelationship = "ATTRIBUTE"
        else:
            modelRelationship = "METRIC"

        isPrimaryKey = fields[code]["isPK"]

        if isPrimaryKey:
            pk_flag = True

        jsonField = GenerateFieldJsonParam(
            id=id,
            name=name,
            code=code,
            fieldType=fieldType,
            precision=precision,
            scale=scale,
            comment=comment,
            pathName=pathName,
            modelRelationship=modelRelationship,
            isPrimaryKey=isPrimaryKey,
            No=No,
        )
        list_fields.append(jsonField)
        No += 1
    if pk_flag:
        return list_fields

    return None


if __name__ == "__main__":
    field_infos = {"test2": {"clsName": "保单", "isPK": True, "isFK": False}}
    # dict_field = GetFieldInfosFromJS()
    res = GenerateJsonFields(field_infos)
    print(res)
