import os
import Queue
import threading

from database.management import DBM

dbm = DBM()
dbm.initialize()

#fileList = os.listdir(".")
#worker = PageAnalyzer(1, "worker-1", Queue.Queue(10), fileList)
#worker.testMain()
print "Program Start"
fileList = os.listdir(".")
jobQueue = Queue.Queue(3500)
producerLock = threading.Lock()
producerLock.acquire()
counter = 1
for entry in fileList:
    print "continue put job" + str(counter)
    jobQueue.put(open(entry, 'r'))
    counter += 1
producerLock.release()

threadCounter = 5
threadList = []
start = clock()
while threadCounter > 0:
    print "Initialize worker"
    worker = PageAnalyzer(threadCounter, "Worker-"+str(threadCounter), jobQueue, [])
    worker.start()
    threadList.append(worker)
    threadCounter -= 1
    

for item in threadList:
    item.join()
end = clock() - start
print "Run time is", end
dbm.export()
