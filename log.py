import logging
from datetime import datetime
import os

logDate = datetime.now().strftime("%Y_%m_%d")
if not os.path.exists("./Log"):
    os.mkdir("./Log")
loggingPath = "./Log"
# logging.basicConfig(level=logging.INFO,format ='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO,format ='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                    ,filename=f'{loggingPath}/dehoopApi_{logDate}.log',encoding="utf-8",filemode="a" )
logger = logging.getLogger(__name__)
