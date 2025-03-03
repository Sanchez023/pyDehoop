from Dehoop import Dehoop
from ParamStruct import ParamOutLineWork,ParamDDLContent
from Table import ReplaceKeyWords,ReplaceKeyWords_spark
from utils.TransFormer import Transerfrom_addColumn,Transerfrom_mappingList,ExtraColumn,ReMappingList


import pandas as pd
import tqdm 
if __name__ == '__main__':
    
    # u = ''
    # passwd = ''

    d = Dehoop("10.1.8.17",30104)
    # d.Login(u,passwd)
    # d.token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJkZWhvb3B1c2VyaWQiOiI2NTcyNTM5NTk3NjQ4MDM1ODQiLCJ0ZW5hbnRpZCI6IjY1NjYwOTExODM5OTc1ODMzNiIsImV4cCI6MTc0MDY1MDgyNCwiaWF0IjoxNzQwMzkxNjI0fQ.4xeTV7RCB3D_dr7tJSDG_9UoVybxAQ7TVZwhkog0ZRB8ct7rnWLi6e3hTgIAq2hFF1g4Dhmcly-R8LDmPu-77g"
    d.userId = '657253959764803584'
    d.tenantid = '656609118399758336'
    projectName = '中台-客户管理其他域模型开发'
    
    parentid = "683001304129208320"
    workspaceId = '656613585694228480'
    
    fromDbId = "661617050451443712"
    toDbId = "682905336633360384"
    
    
   
    df= pd.read_excel("C:/Xiaomi Cloud/Desktop/cbasic-1.xlsx").fillna('')
    
    type = 'SYNC'
    system = 'MS_SCRM'
    
    with open("./log_id.txt","w",encoding="utf-8") as f:
        f.write("")
    for i in tqdm.tqdm(range(len(df)),desc=f"创建{type}中...",total=len(df),colour="GREEN"):
        name = df.iloc[i, 0]
        descr = df.iloc[i, 1]

        TABLENAME = name
        DESCR = descr
        
        match type:
            case 'spark':
                INPARAM = TABLENAME
                p = ParamOutLineWork(parentId=parentid,name="ODS_"+system+"_"+INPARAM+"_ONYARN",descr=DESCR,workspaceId=workspaceId,type="SparkSQL",director = d.userId)
            case 'ODS':
                INPARAM = 'ODS_'+system+'_' + TABLENAME
                p = ParamOutLineWork(parentId=parentid,name = INPARAM,descr=DESCR,workspaceId=workspaceId,director = d.userId)
            case 'STG':
                INPARAM = 'STG_'+system+'_' + TABLENAME
                p = ParamOutLineWork(parentId=parentid,name = INPARAM,descr=DESCR,workspaceId=workspaceId,director = d.userId)
            case 'SYNC':
                INPARAM = 'STG_'+system+'_' + TABLENAME+"_F_1_10"
                p = ParamOutLineWork(parentId=parentid,name = INPARAM,descr=DESCR,workspaceId=workspaceId,director = d.userId,type = "SYNC")
                
        id = d.CreateDDLWork(projectName,p)
        if id is not None:
            try:  
                with open("./log_id.txt","a",encoding="utf-8") as f:
                    f.write(id+"\n")
                
          
                # 同步作业保存
                if type == "SYNC":
            
                    toTableName = "STG_"+system+"_"+TABLENAME
                
                    fromTableName = system+"."+TABLENAME
                
                    column_list:list[dict]= d.GetColumnInfos(projectName,toDbId,toTableName,"dist")
                    column_list_src:list[dict] = d.GetColumnInfos(projectName,fromDbId,fromTableName,"src")
                    mappingList = Transerfrom_mappingList(column_list,fromTableName,toTableName)

                    field,uuid,new_column = ExtraColumn("etl_timestamp",fromTableName,"localtimestamp")
                    mappingList = ReMappingList(mappingList,field,uuid)

                    addColumnList = Transerfrom_addColumn(column_list_src,fromTableName)
                    addColumnList.append(new_column)

                    d.SaveOrUpdateSyncWork(projectName,id,fromDbId,fromTableName,toDbId,toTableName,mappingList,addColumnList)

                    continue
        
                script = d.GenerateDDLScript(projectName,fromDbId,toDbId,system+"."+TABLENAME)
                if type == 'spark':
                    script = ReplaceKeyWords_spark(INPARAM)
                elif type == 'STG':
                    script = ReplaceKeyWords(INPARAM,script,DESCR,False)
                elif type == "ODS":
                    script = ReplaceKeyWords(INPARAM,script,DESCR,True)
                    
                p2 = ParamDDLContent(id= id ,workScript=script)
                d.UpdateDDLWork(projectName,p2)
                if type == 'ODS' or type == "STG":
                    d.ExuteDDLWork(projectName,p2)
            except Exception as e:
                d.DeleteWorkById(projectName,id)
                print(TABLENAME)
    
    
