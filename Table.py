class Column:
    def __init__(self,seqno:int,name:str,type:str,comment:str|None,**kwargs):
        self.seqno = seqno
        self.name = name
        self.type = type
        self.comment = comment
        self.isNull = kwargs.get("isNull")
        self.unique = kwargs.get("unique")
        
        
class DDLStruct:
    def __init__(self,tableName:str,tableCommet:str,columns:list[Column]):
        self.tableName = tableName
        self.tableComment = tableCommet
        self.columns = columns
        
    def __str__(self,)->str:
        SUFFIX = ",\n   "
        script = None
        for column in self.columns:
            if column.isNull:
                str_noNull = None
            else:
                str_noNull = "NOT NULL"
                
            if column.comment is not None:
                column.comment= '"'+column.comment+'"'
                
            column_info = [str(i).upper() for i in (column.name,column.type,str_noNull,column.unique,column.comment) if i is not None]
            line = " ".join(column_info)
            if script is not None:
                script = SUFFIX.join([script,line])
            else:
                script = line
        return script
    
    
    def ToScript(self):
        return ReplaceKeyWords(self.tableName,self.__str__(),self.tableComment)
 
def ReplaceKeyWords(tableName:str,columns_script:str,comment:str,ds:bool=False):
    if ds:
        path = "./DDL/modelSql_ds.sql"
    else:
        path = ".//DDL/modelSql.sql"
        
    with open(path,"r",encoding="utf-8") as f:
        s = f.read()     

        s = s.replace("{{TABLENAME}}",tableName)
        s = s.replace("{{COLUMNS_SCRIPTS}}",columns_script)
        s = s.replace("{{COMMENT}}",comment)
    return s 

def ReplaceKeyWords_spark(tableName:str):
    path = "./DDL/sparkSql.sql"
    with open(path,"r",encoding="utf-8") as f:
        s = f.read()     
        s = s.replace("{{TABLENAME}}",tableName)
    return s 
def ReplaceKeyWords_sparkv2(tableName:str,partition:str,joins:str):
    path = "./DDL/sparkSql_model.sql"
    with open(path,"r",encoding="utf-8") as f:
        s = f.read()     
        s = s.replace("{{TABLENAME}}",tableName)
        s = s.replace("{{PRATITION}}",partition)
        s = s.replace("{{LINKCOLUMNS}}",joins)
        
    return s

def GetColumns(sql_script:str):
    import re
    start_index = 0
    end_index = 0
    for index, item in enumerate(sql_script, start=0):
        if item == "(" and start_index == 0 :
            start_index = index
        if item == ")":
            end_index = index
        str_sql = sql_script[start_index+1:end_index].strip()
  
    return re.sub(r"VARCHAR\(.*\)",'STRING',str_sql)

if __name__ == '__main__':
    pass