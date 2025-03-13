from Dehoop import Dehoop
from ParamStruct import ParamOutLineWork, ParamDDLContent
from Table import ReplaceKeyWords, ReplaceKeyWords_spark
from utils.TransFormer import (
    Transerfrom_addColumn,
    Transerfrom_mappingList,
    ExtraColumn,
    ReMappingList,
)
from utils.Db import MySQLConnector


import pandas as pd
from datetime import datetime
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
    mysqlConnector = MySQLConnector("root","leo130", "localhost", "hbbx")
    dfs = pd.read_excel(
        "C:/Xiaomi Cloud/Desktop/DWD.xlsm",sheet_name="保单共保信息轨迹表",  header=None
    )

    st_frame = dfs
    # st_frame = dfs
    ctableName = st_frame.iloc[0, 2]
    tableName = st_frame.iloc[1, 2]
    originalTableName = st_frame.iloc[1, 5]

    columns = [i for i in st_frame.iloc[3:4, 0:12].fillna("isDelete").values[0]]
    
    data_frame = st_frame.iloc[4:, 0:12].fillna("")
    data_frame.columns = columns

    data_frame = data_frame[data_frame["isDelete"] == 0][["源表物理字段", "主键"]]

    nor_fields = []
    pk_fields = []
    str_fields = ""
    str_keys = ""
    for field in data_frame.values:
        if field[0] != "":
            nor_fields.append(field[0])
        if field[1]: 
            pk_fields.append("DWD." + field[0] + " = ODS." + field[0])

    str_keys = " AND ".join(pk_fields)

    str_fields = " ,".join(nor_fields)
    try:
        sql_execute = f"select parititonFields from tabletmp where tableName = '{originalTableName.split("_")[-1]}';"
        res = mysqlConnector.execute_sql(sql_execute)
    except:
        print(f"表 {originalTableName} 的为多来源表。")
        partitionField = "multiple"
        
    try:
        partitionField = res[0][0]
    except:
        print(f"表 {originalTableName} 的分区字段不存在。")
        partitionField = "ds"
    
    if len(pk_fields) == 0:
        print(f"表 {tableName} 没有主键。")
        pk_fields = ["",""]
        partitionField = "noPK"

    script = (
        f"""
WITH STOCKDATA AS (
    SELECT * FROM {tableName} t WHERE EXISTS (SELECT 1 FROM { originalTableName} o WHERE t.ds = {partitionField} )
)
INSERT OVERWRITE TABLE {tableName} PARTITION (DS) 
SELECT 
    DWD.*
FROM 
    STOCKDATA DWD 
LEFT JOIN  
    {originalTableName} ODS 
ON 
    {str_keys} 
WHERE 
    {nor_fields[0]} IS NULL  -- 请将 some_field 替换为实际字段名
UNION ALL
SELECT
    {str_fields},
    $env.current_time as elt_timestamp,
    {partitionField} AS DS  -- 确认 PRATITION 是否正确拼写为 PARTITION
FROM
    {originalTableName} ods
"""
    )
    mysqlConnector.execute_sql_WithouReturn(f"insert into dwd_script values ('{tableName}', '{script}','{partitionField}','{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}')")
    
    print(f"表 {tableName} 的脚本已插入到 dwd_script 表中。")