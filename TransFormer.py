from uuid import uuid4
def generateUUid(isHyphen:bool=True):
    if isHyphen:
        return uuid4().__str__()
    return uuid4().hex


def Transerfrom_addColumn(origin_list:list[dict],tableName):
    addColumns = []
    for dict_column in origin_list:
        dict_column["table"] = tableName
        dict_column["$__seq_ID"] = "__seq_ID_"+generateUUid()
        dict_column["addColumn"] = None
        addColumns.append(dict_column)
    
    return addColumns



def Transerfrom_mappingList(origin_list:list[dict],fromTable:str,toTableName:str):
    mappingList =[]
    for column in origin_list:

        mapping_dict = {}

        mapping_dict["from"] = str(column["field"]).upper()
        mapping_dict["fromApiFieldId"] = fromTable
        mapping_dict["to"] = column["field"]
        mapping_dict["fromTable"] = fromTable
        mapping_dict["toTable"] = toTableName
        mappingList.append(mapping_dict)
        
    return mappingList

def ExtraColumn(field:str,table:str,func:str):
    uuid = generateUUid(False)
    dict_extraColumn= {"addColumn": True,
            "field": uuid,
            "showField": "-",
            "fieldtype": "-",
            "type": "-",
            "table": table,
            "$__seq_ID": "__seq_ID_"+generateUUid(),
            "fromTransform": func}
    return field,uuid,dict_extraColumn

def ReMappingList(origin_list:list[dict],field:str,uuid:str):
    temp = -1
    for index,map in enumerate(origin_list):
        if map['to'] == field:
            temp = index
    origin_list[temp]['from'] = uuid
    return origin_list
