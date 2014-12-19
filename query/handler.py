import sys
import logging
from datetime import date, datetime, timedelta

import requests

from loggers import logger_query

class Query:
    """Providing multiple query functions"""

    def __init__(self, endpoint="https://data.cityofnewyork.us/resource/eabe-havv.json"):
        self._endpoint = endpoint

     
    def getAllCaseList(self, paraDict):
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
        while length >= limit:
            r = requests.get(self._endpoint, params=paraDict)
            objList = r.json()
            length = len(objList)
            result.extend(objList)
            offset += limit
            paraDict['$offset'] = offset
        
        return result

    def getAllClosedCaseSet(self, startDate=None):
        # startDate should be an instance of datetime
        p = {
            "$select": "complaint_number, date_entered",
            "$where": "status = \'CLOSED\'", 
        } 

        if startDate != None and isinstance(startDate, datetime):
            p["$where"] = p["$where"] + " AND date_entered >= \'" + \
                                                startDate.strftime("%m/%d/%Y") + "\'"
        
        objList = self.getAllCaseList(p)

        closeCase = set()

        if startDate == None:
            for obj in objList:
                closeCase.add(obj["complaint_number"])
        elif isinstance(startDate, datetime):
            for obj in objList:
                if datetime.strptime(obj["date_entered"], "%m/%d/%Y") >= startDate:
                    closeCase.add(obj["complaint_number"])
        else:
            logger_query.error("Error happend on module \
                        query.handler.getAllClosedCaseSet: \
                                unrecognized type for startDate")

        return closeCase

    def getLastNDayCaseList(self, nday, status):
        # Because the date_entered field is stored as text, so we cannot query 
        # using > or <. We are going to query day by day   
        
        # Initialization
        oneday = timedelta(days=1)
        queryDate = date.today() - oneday
        p = {
            "$select": "complaint_number",
            "$where": "date_entered = \'" + queryDate.strftime("%m/%d/%Y") \
                        + "\' AND status = \'" + status + "\'"         
        }
        
        objList = []
        while nday > 0:
            objList.extend(self.getAllCaseList(p))
            queryDate -= oneday
            p['$where'] = "date_entered = \'" \
                            + queryDate.strftime("%m/%d/%Y") \
                            + "\' AND status = \'" + status + "\'"
            nday -= 1

        complaintNumList = []
        for obj in objList:
            complaintNumList.append(obj['complaint_number'])

        return complaintNumList

    def getRecentActiveCaseNum(self, start, end):
        # Initialization
        start = str(start)
        end = str(end)
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
