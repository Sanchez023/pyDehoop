from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

class MySQLConnector:
    """
    MySQLConnector类用于管理与MySQL数据库的连接和执行SQL语句。
    
    属性:
        engine (sqlalchemy.engine.Engine): SQLAlchemy引擎对象，用于与数据库建立连接。
        session (sqlalchemy.orm.session.Session): SQLAlchemy会话对象，用于执行SQL语句。
    """

    def __init__(self, username: str, password: str, host: str, database: str):
        """
        初始化MySQLConnector对象，建立与MySQL数据库的连接。

        参数:
            username (str): MySQL数据库的用户名。
            password (str): MySQL数据库的密码。
            host (str): MySQL数据库的主机地址。
            database (str): 要连接的数据库名称。
        """
        # 创建数据库连接URL
        db_url = f"mysql+pymysql://{username}:{password}@{host}/{database}"
        
        try:
            # 创建SQLAlchemy引擎
            self.engine = create_engine(db_url, echo=True)
            # 创建会话工厂
            Session = sessionmaker(bind=self.engine)
            # 创建会话对象
            self.session = Session()
        except exc.SQLAlchemyError as e:
            # 捕获SQLAlchemy异常并打印错误信息
            print(f"数据库连接失败: {e}")
            raise
    
    def execute_sql_WithouReturn(self,sql:str):
        """
        执行指定的SQL语句。

        参数:
            sql (str): 要执行的SQL语句。

        返回:
            result (list): SQL查询的结果集（如果有）。

        异常:
            exc.SQLAlchemyError: 如果执行SQL语句时发生错误。
        """
        try:
            # 使用会话对象执行SQL语句
            script = text(sql)
            result = self.session.execute(script)
            # 提交事务
            self.session.commit()
        except exc.SQLAlchemyError as e:
            # 捕获SQLAlchemy异常并打印错误信息
            print(f"SQL执行失败: {e}")
            # 回滚事务
            self.session.rollback()
            raise
        
    def execute_sql(self, sql: str):
        """
        执行指定的SQL语句。

        参数:
            sql (str): 要执行的SQL语句。

        返回:
            result (list): SQL查询的结果集（如果有）。

        异常:
            exc.SQLAlchemyError: 如果执行SQL语句时发生错误。
        """
        try:
            # 使用会话对象执行SQL语句
            script = text(sql)
            result = self.session.execute(script)
            # 提交事务
            self.session.commit()
            # 返回查询结果
            print(result)
            return result.fetchall()
        except exc.SQLAlchemyError as e:
            # 捕获SQLAlchemy异常并打印错误信息
            print(f"SQL执行失败: {e}")
            # 回滚事务
            self.session.rollback()
            raise

    def close(self):
        """
        关闭数据库连接。
        """
        # 关闭会话
        self.session.close()

# 示例用法
if __name__ == "__main__":
    # 创建MySQLConnector对象
    connector = MySQLConnector(username='root', password='xxx', host='localhost', database='xxx')
    
    try:
        # 执行SQL查询
        result = connector.execute_sql("SELECT * FROM xxx;")
        # 打印查询结果
        for row in result:
            print(row)
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        # 关闭数据库连接
        connector.close()