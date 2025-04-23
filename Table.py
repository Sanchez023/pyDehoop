class Column:
    """列定义类\n
    用于定义表中的列及其属性
    """
    def __init__(self,seqno:int,name:str,type:str,comment:str|None,**kwargs):
        """初始化列对象\n
        参数:
            seqno: 列序号
            name: 列名
            type: 列类型
            comment: 列注释
            kwargs: 其他参数，包括isNull和unique
        """
        self.seqno = seqno
        self.name = name
        self.type = type
        self.comment = comment
        self.isNull = kwargs.get("isNull")
        self.unique = kwargs.get("unique")
        
        
class DDLStruct:
    """DDL结构体类\n
    用于构建表的DDL语句
    """
    def __init__(self,tableName:str,tableCommet:str,columns:list[Column]):
        """初始化DDL结构体\n
        参数:
            tableName: 表名
            tableCommet: 表注释
            columns: 列对象列表
        """
        self.tableName = tableName
        self.tableComment = tableCommet
        self.columns = columns
        
    def __str__(self,)->str:
        """将DDL结构体转换为字符串\n
        返回:
            str: 格式化后的列定义字符串
        """
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
        """生成完整的DDL脚本\n
        返回:
            str: 完整的建表DDL语句
        """
        return ReplaceKeyWords(self.tableName,self.__str__(),self.tableComment)
 
def ReplaceKeyWords(tableName:str,columns_script:str,comment:str,ds:bool=False):
    """替换SQL模板中的关键字\n
    参数:
        tableName: 表名
        columns_script: 列定义脚本
        comment: 表注释
        ds: 是否使用数据服务模板，默认为False

    返回:
        str: 替换关键字后的完整SQL脚本
    """
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
    """替换Spark SQL模板中的关键字\n
    参数:
        tableName: 表名

    返回:
        str: 替换表名后的Spark SQL脚本
    """
    path = "./DDL/sparkSql.sql"
    with open(path,"r",encoding="utf-8") as f:
        s = f.read()     
        s = s.replace("{{TABLENAME}}",tableName)
    return s 

def ReplaceKeyWords_sparkv2(tableName:str,partition:str,joins:str):
    """替换Spark SQL模型模板中的关键字\n
    参数:
        tableName: 表名
        partition: 分区字段
        joins: 关联字段

    返回:
        str: 替换关键字后的Spark SQL模型脚本
    """
    path = "./DDL/sparkSql_model.sql"
    with open(path,"r",encoding="utf-8") as f:
        s = f.read()     
        s = s.replace("{{TABLENAME}}",tableName)
        s = s.replace("{{PRATITION}}",partition)
        s = s.replace("{{LINKCOLUMNS}}",joins)
        
    return s

def GetColumns(sql_script:str):
    """从SQL脚本中提取列定义部分\n
    参数:
        sql_script: SQL脚本字符串

    返回:
        str: 提取并处理后的列定义字符串，将VARCHAR类型转换为STRING
    """
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