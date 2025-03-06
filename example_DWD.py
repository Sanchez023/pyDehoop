from Dehoop import Dehoop
from ParamStruct import ParamOutLineWork, ParamDDLContent
from Table import ReplaceKeyWords, ReplaceKeyWords_spark
from utils.TransFormer import (
    Transerfrom_addColumn,
    Transerfrom_mappingList,
    ExtraColumn,
    ReMappingList,
)


import pandas as pd
import tqdm

if __name__ == "__main__":

    # u = ''
    # passwd = ''

    # d = Dehoop("10.1.8.17",30104)
    # d.Login(u,passwd)
    # d.token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJkZWhvb3B1c2VyaWQiOiI2NTcyNTM5NTk3NjQ4MDM1ODQiLCJ0ZW5hbnRpZCI6IjY1NjYwOTExODM5OTc1ODMzNiIsImV4cCI6MTc0MDY1MDgyNCwiaWF0IjoxNzQwMzkxNjI0fQ.4xeTV7RCB3D_dr7tJSDG_9UoVybxAQ7TVZwhkog0ZRB8ct7rnWLi6e3hTgIAq2hFF1g4Dhmcly-R8LDmPu-77g"
    # d.userId = '657253959764803584'
    # d.tenantid = '656609118399758336'
    # projectName = '中台-客户管理其他域模型开发'

    # parentid = "683001304129208320"
    # workspaceId = '656613585694228480'

    # fromDbId = "661617050451443712"
    # toDbId = "682905336633360384"

    dfs = pd.read_excel(
        "C:/Xiaomi Cloud/Desktop/DWD.xlsm", sheet_name="保单信息表", header=None
    )

    # for sheet in dfs:
    #     if sheet == '字段标准':
    #         continue
    # st_frame = dfs[sheet]
    st_frame = dfs
    ctableName = st_frame.iloc[0, 2]
    tableName = st_frame.iloc[1, 2]
    originalTableName = st_frame.iloc[1, 5]

    columns = [i for i in st_frame.iloc[3:4, :].fillna("isDelete").values[0]]

    data_frame = st_frame.iloc[4:, :].fillna("")
    data_frame.columns = columns

    data_frame = data_frame[data_frame["isDelete"] == 0][["标准物理字段", "主键"]]

    nor_fields = []
    pk_fields = []
    str_fields = ""
    str_keys = ""
    for field in data_frame.values:
        if field[0] != "":
            nor_fields.append(field[0])
        if field[1]:
            pk_fields.append("DWD."+field[0]+ " = ODS." + field[0])

    str_keys = " AND ".join(pk_fields)
    
    str_fields = " ,".join(nor_fields)

    print(
        f"""
INSERT OVERWRITE INTO TABLE {tableName} PARTITION (DS) 
SELECT 
    {str_fields} 
FROM 
    {tableName} dwd 
LEFT JOIN  
    {originalTableName} ods 
ON 
    {str_keys} 
WHERE 
    ods.{pk_fields[0]} IS NULL  -- 请将 some_field 替换为实际字段名
UNION ALL
SELECT
    {str_fields},
    PARTITION AS DS  -- 确认 PRATITION 是否正确拼写为 PARTITION
FROM
    {originalTableName} ods
"""
    )
