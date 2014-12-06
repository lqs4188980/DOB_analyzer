import sys
import datetime
from datetime import date

import request

class Query:
    """Providing multiple query functions"""

    def __init__(self, endpoint)
	self._endpoint = endpoint

     
    def __getAllCaseList(self, paraDict):
	# Initialize parameter
	limit = 50000
	offset = 0
	# url_endpoint = "https://data.cityofnewyork.us/resource/eabe-havv.json"
	
	# Construct Query
	paraDict['$limit'] = limit
	paraDict['$offset'] = offset

	# Construct request and query
	length = sys.maxint
	result = []
	while length == limit:
	    r = requests.get(self._endpoint, params=p)
	    objList = r.json()
	    length = len(objList)
	    result.extend(objList)
	    p['$offset'] = (offset += limit)
	
	return result

    def getAllClosedCaseSet(self):
	p = {
	    "$select": "complaint_number",
	    "$where": "status = CLOSED", 
	} 
	
	objList = self.__getAllCaseList(p)

	closeCase = set()
	for obj in objList:
	    closeCase.add(obj['complaint_number'])

	return closeCase

    def getLastNDayActiveCaseList(self, nday, status):
	# Because the date_entered field is stored as text, so we cannot query 
	# using > or <. We are going to query day by day   
	
	# Initialization
	oneday = datetime.timedelta(days=1)
	queryDate = date.today() - oneday
	p = {
	    "$select": "complaint_number",
	    "$where": "date_entered = \'" + queryDate.strftime("%m/%d/%y") \
			+ "\' AND status = \'" + status + "\'" 	
	}
	
	objList = []
	while nday > 0:
	    objList.extend(self.__getAllCaseList(p))
	    p['$where'] = "date_entered = \'" \ 
			    + (queryDate -= oneday).strftime("%m/%d/%y") \
			    + "\' AND status = \'" + status + "\'"
	    nday -= 1

	complaintNumList = []
	for obj in objList:
	    complaintNumList.add(obj['complaint_number'])

	return complaintNumList

    def getRecentCaseNumInARange(self, start, end):
	# Initialization
	p = {
	    "$select": "complaint_number", 
	    "$where": "complaint_number > \'" + start \ 
		    + "\' AND complaint_number < \'" + end + "\'",
	    "$order": "complaint_number DESC",
	    "$limit": 1
	}	

	r = requests.get(self._endpoint, params=p)
	if len(r.json()) > 0:
	    return r.json()[0]['complaint_number']
	else:
	    return None
