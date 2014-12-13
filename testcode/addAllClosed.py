print __package__
# import requests
# 
# from query.handler import Query
# from database.management import DBM
# 
# p = {
#     "$select": "complaint_number", 
#     "$where": "status = \'CLOSED\'"
# }
# 
# q = Query("https://data.cityofnewyork.us/resource/eabe-havv.json")
# 
# close = q.getAllCaseList(p)
# 
# dbm = DBM()
# dbm.initialize()
# 
# for obj in close:
#     dbm.putClose(obj['complaint_number'])
