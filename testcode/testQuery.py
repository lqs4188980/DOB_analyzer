from datetime import date
from query.handler import Query

q = Query()

print "****************** Test getAllClosedCaseList ********************"
print len(q.getAllClosedCaseSet())

print "****************** Test getLastNDayCaseList *********************"
print q.getLastNDayCaseList(1, "ACTIVE")

print "****************** Test getRecentActivecaseNum *********************"
print q.getRecentActiveCaseNum(1000000, 2000000)
print q.getRecentActiveCaseNum(0, 1)
