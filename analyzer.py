import re
import os
import threading
import Queue
import logging
import datetime
import copy
import sys, traceback
from time import clock
from lxml import html
from database.management import DBM
from loggers import logger_analyzer, logger_prt

class PageAnalyzer (threading.Thread):
    '''Analyze and parse information from DOB pages'''

    fieldPattern = re.compile("([0-9A-Za-z #]+):")

    patternDict = {
        "title": re.compile('Overview for Complaint #:(?P<complaint>[0-9]{7}) = (?P<status>(CLOSED)|(ACTIVE)|(RESOLVED)|(REFERRED))'), 
        "mainInfoList": {
           "Complaint at": re.compile("Complaint at:(\xa0){0,}(?P<address>.*)(\xa0){0,}"),
            "BIN": re.compile("BIN: (\xa0){0,}(?P<bin>\d*)(\xa0){0,}"),
            "Borough": re.compile("Borough:(\xa0){0,}(?P<borough>.*)(\xa0){0,}"), 
            "ZIP": re.compile("ZIP:(\xa0){0,}(?P<zip>.*)(\xa0){0,}"),
        },
        "contentList": {
            "Block": re.compile(".*?(\xa0){0,}(?P<number>[0-9]*)(\xa0){0,}$"),
            "Lot": re.compile(".*?(\xa0){0,}(?P<number>[0-9]*)(\xa0){0,}$"),
            "Last Inspection": re.compile("(\xa0){0,}(?P<inspectionDate>[0-9/]+)(\xa0){0,}"),
            "Disposition": re.compile("(\xa0){0,}(?P<disposition>.*)(\xa0){0,}"),
            "Category Code": re.compile("(\xa0){0,}(?P<category>\w{2})(\xa0){0,}"),
            "Received": re.compile("(\xa0){0,}(?P<received>[0-9/]+)(\xa0){0,}"),
            "Owner": re.compile("(\xa0){0,}(?P<owner>.*)(\xa0){0,}$"),
            "DOB Violation #": re.compile("(\xa0){0,}(?P<dobVio>.*)(\xa0){0,}$"),
            "ECB Violation #": re.compile("(\xa0){0,}(?P<ecbVio>.*)(\xa0){0,}$"),
            "Comments": re.compile("(\xa0){0,}(?P<comments>.*)(\xa0){0,}"),
        }
    }   

    mainInfoFieldList = ["Complaint at", "BIN", "Borough", "ZIP"]

    contentFieldList = [
        "Category Code", 
        "Received", 
        "Block", 
        "Lot", 
        "Owner", 
        "Last Inspection", 
        "Disposition",
        "Comments", 
        "DOB Violation #", 
        "ECB Violation #"
    ]

    infoTemplate = {
        'Complaint Number': '',
        'Status': '',
        'BIN': '',
        'Category Code': '',
        'Lot': '',
        'Block': '',
        'ZIP': '',
        'Received': '',
        'DOB Violation #': '',
        'Comments': '',
        'Owner': '',
        'Last Inspection': '',
        'Disposition': '',
        'Borough': '',
        'Complaint at': '',
        'ECB Violation #': '',        
    }

    def __init__ (self, rawQ, taskQ, threadID=None, name=None):
        threading.Thread.__init__(self)
        self.ThreadID = threadID
        self.Name = name
        self._JobQueue = rawQ
        self._TaskQueue = taskQ
        self._queueLock = threading.Lock()
        self._exit = False

        # Dictionary for store page info
        self.info = copy.copy(PageAnalyzer.infoTemplate)

        # DB object
        self._dbm = DBM()
        # self._dbm.initialize()
    
        # Test file list
        # self._fileList = fileList

    def testMain(self, filename):
        f = open(filename, 'r')
        self._doc = html.fromstring(f.read())
        if self.__titleParser():
            print "************", self.info['Complaint Number'], "*************"
            self.__mainInfoParser()
            self.__contentParser()
            self.printInfo()
            self.__cleanUp()
            

        

    def run(self):
        while True:
            try:
                # Get data from Queue
                self._rawData = self.__getRawDataFromQueue()

                if self._rawData["text"] == "":
                    # Fail to get raw data, needs to add complaint number back and log
                    logger_analyzer.error("Cannot get page content for complaint \
                                            number: %s"% self._rawData["id"])
                    logging.error("Cannot get page content for complaint number: ", \
                                                                str(self._rawData["id"]))
                    self.__insertToTaskQueue("Cannot get page content")

                else:
                    # Preprocessing Data
                    self._doc = html.fromstring(self._rawData["text"]) 
            
                    # Parse title
                    if self.__titleParser():
                        # If title is valid, get status and decide whether continue parse
                        self.__parseRequiredContent()
                    else:
                        # The case format doesn't correct
                        self.__insertToTaskQueue("Fail to Parse Title")
                        logger_analyzer.error("Fail to Parse Title: %s"% \
                                                                    self._rawData["id"])
                    
                self._TaskQueue.task_done()
                logger_prt.debug('Task done ' + str(self._rawData['id']))
                self.__cleanUp()
            except Exception as e:
                logger_analyzer.error("Critical error, Analyzer thread failed.")
                logger_analyzer.error(repr(e))
                logger_analyzer.error(traceback.format_exc())
                logger_analyzer.error(sys.exc_info())

    def __parseRequiredContent(self):
        if self.info['Status'] == "RESOLVED":
            # If case if RESOLVED, we need to parse all the infomation and 
            # insert into database
            if self.__mainInfoParser() and self.__contentParser(): 
                # Has any violation #
                if self.__hasViolationNumber():
                    self.__insertToComplaint()
                    # Delete this case from Warehouse
                    self._dbm.deleteWarehouseCase(self.info['Complaint Number'])
                else:
                    # if no violation #, insert into or update Warehouse
                    # Mark this case as CLOSED
                    self.info['Status'] = "CLOSED"
                    self._dbm.putWarehouseCase(self.info)

            else:
                # When parsing date error
                # Mark this case as CLOSED in advance, and if re-parse
                # success, we will delete it 
                self.info['Status'] = "CLOSED"
                self._dbm.putWarehouseCase(self.info)
                self.__insertToTaskQueue("Fail to parse data")
                logger_analyzer.error("Fail to parse data: %s"% self._rawData["id"])

        elif self.info['Status'] == "ACTIVE":
            # If case is still ACTIVE, we don't need to parse it
            # Also, we add it to Warehouse
            self._dbm.putWarehouseCase(self.info)

        else:
            # For CLOSED and REFERRED or any other cases, mark as CLOSED 
            # and put them into Warehouse
            self.info['Status'] = "CLOSED"
            self._dbm.putWarehouseCase(self.info)
                    
                    
    def __hasViolationNumber(self):
        return (self.info['DOB Violation #'] != '' or \
                self.info['ECB Violation #'] != '')

    def __getRawDataFromQueue(self):
        rawData = self._JobQueue.get()
        
        return rawData

    def __titleParser(self):
        title = self._doc.cssselect('title')
        if len(title) != 0:
            m = PageAnalyzer.patternDict['title'].match(title[0].text_content())
            if m == None:
                # If Not Match, there is some error, we need to log error and deal it
                if "CAPTCHA" in self._doc.text_content():
                    logger_analyzer.error("Get CAPTCHA page for " + self._rawData['id'])
                else:
                    self.__logError("title", title[0])
                return False
            else:
                self.info['Complaint Number'] = m.group('complaint')
                self.info['Status'] = m.group('status')
                return True

        return False

    def __mainInfoParser(self):
        mainInfoList = self._doc.find_class('maininfo')
        if len(mainInfoList) == 0:
            return False
        mainInfoPatterns = PageAnalyzer.patternDict['mainInfoList']
        fieldList = copy.copy(PageAnalyzer.mainInfoFieldList)

        for item in mainInfoList:
            foundField = ""
            for field in fieldList:
                if field in item.text_content():
                    foundField = field
                    m = mainInfoPatterns[field].match(item.text_content())
                    if m == None:
                        self.__logError(field, item) 
                        return False
                    else:
                        self.info[field] = m.group(2)
                        break
            if foundField != "":
                fieldList.remove(foundField)

        return True


    def __contentParser(self):
        contentList = self._doc.find_class('content')
        length = len(contentList)
        if length == 0:
            return False
        contentPatterns = PageAnalyzer.patternDict['contentList']
    
        index = 0
        while index < length:
            entry = contentList[index]
            m = PageAnalyzer.fieldPattern.match(entry.text_content())
            if m != None:
                field = m.group(1)
                if field in PageAnalyzer.contentFieldList:
                    if field != "Block" and field != "Lot":
                        index += 1
                        entry = contentList[index]
                    m = contentPatterns[field].match(entry.text_content())
                    if m == None:
                        self.__logError(field, entry)
                        return False
                    else:
                        self.info[field] = m.group(2)
            
            index += 1

        return True
        
                
        

    def __insertToTaskQueue(self, error=""):
        # Insert ACTIVE case number back to pool
        task = {
            "id": self._rawData["id"],
            "url": self._rawData["url"],
        }
        if error != "":
            task["error"] = error
        self._TaskQueue.put(task)
        logger_prt.debug('Adding ' + task['id'])
        

    def __cleanUp(self):
        self.info = copy.copy(PageAnalyzer.infoTemplate)
        self._doc = None

    def __logError(self, field, htmlNode):
        logger_analyzer.error("Some error happens when parsing " 
            + field 
            + " for " 
            + self.info['Complaint Number'] 
            + ": " 
            + htmlNode.text_content())

    def __insertToComplaint(self):
        # Insert current page info into DB
        self.__convertDatetime()
        self._dbm.putResolve(self.info)

    def __convertDatetime(self):
        if self.info['Received'] != '':
            self.info['Received'] = datetime.datetime.strptime(self.info['Received'], "%m/%d/%Y")
        else:
            self.info['Received'] = None
        if self.info['Last Inspection'] != '':
            self.info['Last Inspection'] = datetime.datetime \
                    .strptime(self.info['Last Inspection'], "%m/%d/%Y")
        else:
            self.info['Last Inspection'] = None

    def printInfo(self):
        #print self.info
        keySet = self.info.keys()
        for key in keySet:
            print key, ": ", self.info[key]

