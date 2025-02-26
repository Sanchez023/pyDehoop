# 多线程处理接口案例
from concurrent import futures
from queue import Queue
import threading

import time
q = Queue()
stop_event = threading.Event()
 
task_queue = ["task1","task2","task1"]
def task(taskName:str):
    print(f'task :{taskName} is starting...')
 
    import random
    
    sleepTime = random.randint(4,10)
    print(f"task {taskName}wait time :{sleepTime}")
    time.sleep(sleepTime)
    print(f'task :{taskName} end')
    q.put(taskName)
    
    
def consumer(queue:Queue,stop_event):
    while not stop_event.is_set():
        time.sleep(1)
        if q.empty():
            continue
        item = queue.get(timeout=1)
        print(f'Consumed: {item}')
        q.task_done()
            
            
def excute(workNum:int):
    works = workNum
    consumer_thread = threading.Thread(target=consumer,args=(q,stop_event))
    consumer_thread.start()
    with futures.ThreadPoolExecutor(works) as executor:
        executor.map(task,task_queue)
   

    
    stop_event.set()
    consumer_thread.join()

if __name__ == "__main__":
    excute(3)