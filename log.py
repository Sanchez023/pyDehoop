import logging
from datetime import datetime
import os

# 获取当前日期作为日志文件名的一部分，格式为年_月_日
logDate = datetime.now().strftime("%Y_%m_%d")
# 检查日志目录是否存在，不存在则创建
if not os.path.exists("./Log"):
    os.mkdir("./Log")
# 设置日志文件路径
loggingPath = "./Log"
# 注释掉的基本配置（不输出到文件）
# logging.basicConfig(level=logging.INFO,format ='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 配置日志，设置日志级别为INFO，指定日志格式，输出到文件
logging.basicConfig(level=logging.INFO,format ='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    ,filename=f'{loggingPath}/dehoopApi_{logDate}.log',encoding="utf-8",filemode="a" )
# 创建日志记录器
logger = logging.getLogger(__name__)
