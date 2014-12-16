from datetime import date, datetime

import requests

from query.handler import Query
from database.management import DBM

#p = {
#    "$select": "complaint_number, date_entered", 
#    "$where": "status = \'ACTIVE\'"
#}
#
#q = Query("https://data.cityofnewyork.us/resource/eabe-havv.json")
#
#active = q.getAllCaseList(p)
#print len(active)
#if len(active) < 10:
#    print active
#    exit()

dbm = DBM()
#dbm.initialize()
#for obj in active:
#    dbm.putActive(obj)
#print dbm.getRecentActiveCaseNum(4000000, 5000000)
#q = Query("https://data.cityofnewyork.us/resource/eabe-havv.json")
#print len(q.getAllClosedCaseSet())
#print len(q.getAllClosedCaseSet())

#print len(dbm.getNewClosedCase())
#print dbm.getNDayActiveCase(3)

print "***************** Test getResolve *****************"
print dbm.getResolve(4036026)

print "***************** Test getAllResolve **********************"
print dbm.getAllResolve()

print "***************** Test getResolveByInspectionDateRange *********************"
print "Test by web access"

print "***************** Test putWarehouseCase ******************"
dbm.putWarehouseCase({'Complaint Number': "1111111", 'Date Entered': date.today(), 'Status': "CLOSED"})

print "***************** Test getActive ******************"
print dbm.getWarehouseCase(1111111)

print "***************** Test deleteWarehouseCase *********************"
dbm.deleteWarehouseCase(1111111)
print dbm.getWarehouseCase(1111111)

print "***************** Test getNewClosedCase ***********"
print len(dbm.getNewClosedCaseFromADate(datetime.strptime("12/01/2014", "%m/%d/%Y")))

print "***************** Test getNDayActiveCase **********"
print len(dbm.getNDayActiveCase(0))

print "***************** Test getRecentActiveCaseNum *******************"
print dbm.getRecentActiveCaseNum(4000000, 5000000)
