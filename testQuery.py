from datetime import date, datetime
from query.handler import Query

q = Query()

print "****************** Test getAllClosedCaseList ********************"
print len(q.getAllClosedCaseSet())
print q.getAllClosedCaseSet(datetime.strptime("12/04/2014", "%m/%d/%Y"))

print "****************** Test getLastNDayCaseList *********************"
print q.getLastNDayCaseList(1, "ACTIVE")

print "****************** Test getRecentActivecaseNum *********************"
print q.getRecentActiveCaseNum(1000000, 2000000)
print q.getRecentActiveCaseNum(0, 1)
