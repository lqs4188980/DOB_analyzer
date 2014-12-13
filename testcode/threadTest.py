import os
import Queue
import threading
from time import clock
from analyzer import PageAnalyzer

from database.management import DBM

dbm = DBM()
dbm.initialize()

#fileList = os.listdir(".")
#worker = PageAnalyzer(1, "worker-1", Queue.Queue(10), fileList)
#worker.testMain()
print "Program Start"
fileList = os.listdir("grabPages/")
jobQueue = Queue.Queue(10)
producerLock = threading.Lock()
producerLock.acquire()
jobQueue.put({'id': "4036020", 'url': "", 'text': open("grabPages/4036020", 'r').read()})
jobQueue.put({'id': "4036021", 'url': "", 'text': open("grabPages/4036021", 'r').read()})
jobQueue.put({'id': "4036022", 'url': "", 'text': open("grabPages/4036022", 'r').read()})
jobQueue.put({'id': "4036023", 'url': "", 'text': open("grabPages/4036023", 'r').read()})
jobQueue.put({'id': "4036024", 'url': "", 'text': open("grabPages/4036024", 'r').read()})
jobQueue.put({'id': "4036025", 'url': "", 'text': open("grabPages/4036025", 'r').read()})
jobQueue.put({'id': "4036026", 'url': "", 'text': open("grabPages/4036026", 'r').read()})
jobQueue.put({'id': "4036027", 'url': "", 'text': open("grabPages/4036027", 'r').read()})
jobQueue.put({'id': "4036028", 'url': "", 'text': open("grabPages/4036028", 'r').read()})
jobQueue.put({'id': "4036029", 'url': "", 'text': open("grabPages/4036029", 'r').read()})
# counter = 1
# for entry in fileList:
#     print "continue put job" + str(counter)
#     jobQueue.put(open("grabPages/" + entry, 'r'))
#     counter += 1
producerLock.release()

taskQueue = Queue.Queue(10)
taskQueue.put({'id': 4036020, 'url': ""})
taskQueue.put({'id': 4036020, 'url': ""})
taskQueue.put({'id': 4036020, 'url': ""})
taskQueue.put({'id': 4036020, 'url': ""})
taskQueue.put({'id': 4036020, 'url': ""})
taskQueue.put({'id': 4036020, 'url': ""})
taskQueue.put({'id': 4036020, 'url': ""})
taskQueue.put({'id': 4036020, 'url': ""})
taskQueue.put({'id': 4036020, 'url': ""})
taskQueue.put({'id': 4036020, 'url': ""})

threadCounter = 5
threadList = []
start = clock()
while threadCounter > 0:
    print "Initialize worker"
    worker = PageAnalyzer(threadCounter, "Worker-"+str(threadCounter), jobQueue, taskQueue)
    worker.start()
    threadList.append(worker)
    threadCounter -= 1
    

for item in threadList:
    item.join()
end = clock() - start
print "Run time is", end
#dbm.export()
