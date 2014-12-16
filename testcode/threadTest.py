import os
import Queue
import threading
from time import clock
from analyzer import PageAnalyzer

from database.management import DBM

#dbm = DBM()
#dbm.initialize()

#fileList = os.listdir(".")
#worker = PageAnalyzer(1, "worker-1", Queue.Queue(10), fileList)
#worker.testMain()
print "Program Start"
fileList = os.listdir("grabPages/")
jobQueue = Queue.Queue(3000)
taskQueue = Queue.Queue(3000)
producerLock = threading.Lock()
counter = 1
producerLock.acquire()
for entry in fileList:
    print "continue put job" + str(counter)
    jobQueue.put({'id': entry, 'url': "", 'text': open("grabPages/" + entry, 'r').read()})
    taskQueue.put({'id': entry, 'url': ""})
    counter += 1
producerLock.release()

threadCounter = 5
threadList = []
start = clock()
while threadCounter > 0:
    print "Initialize worker"
    worker = PageAnalyzer(jobQueue, taskQueue, threadCounter, "Worker-"+str(threadCounter))
    worker.start()
    threadList.append(worker)
    threadCounter -= 1
    

for item in threadList:
    item.join()
end = clock() - start
print "Run time is", end
#dbm.export()
