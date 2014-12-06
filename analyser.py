import re
import os
import threading
import Queue
import logging
import datetime
import copy
from time import clock

from lxml import html

from database.management import DBM

class PageAnalyzer (threading.Thread):
    '''	Analyze and parse information from DOB pages'''

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
	    "Category Code": re.compile("(\xa0){0,}(?P<category>\w{2})(\xa0){0,}"),
	    "Received": re.compile("(\xa0){0,}(?P<received>[0-9/]+)(\xa0){0,}"),
	    "Owner": re.compile("(\xa0){0,}(?P<owner>.*)(\xa0){0,}$"),
	    "DOB Violation #": re.compile("(\xa0){0,}(?P<dobVio>.*)(\xa0){0,}$"),
	    "ECB Violation #s": re.compile("(\xa0){0,}(?P<ecbVio>.*)(\xa0){0,}$"),
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
	"Comments", 
	"DOB Violation #", 
	"ECB Violation #s"
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
   	'Borough': '',
   	'Complaint at': '',
   	'ECB Violation #s': '',	
    }

    def __init__ (self, threadID, name, q, fileList):
	threading.Thread.__init__(self)
	self.ThreadID = threadID
	self.Name = name
	self.JobQueue = q
	self.queueLock = threading.Lock()
	self.exit = False

	# Dictionary for store page info
	self.info = copy.copy(PageAnalyzer.infoTemplate)

	# DB object
	dbm = DBM()
    
	# Test file list
	self.fileList = fileList

    def testMain(self):
	for eachEntry in self.fileList:
	    f = open(eachEntry, 'r')
	    self.doc = html.fromstring(f.read())
	    if self.__titleParser():
		print "************", self.info['Complaint Number'], "*************"
		self.__mainInfoParser()
		self.__contentParser()
		self.printInfo()
		self.__cleanUp()
	    

	

    def run(self):
	while not self.JobQueue.empty():

	    # Get data from Queue
	    pageContent = self.__getRawDataFromQueue()

	    if pageContent == "":
		# Fail to get raw data, needs to add complaint number back and log
		continue

	    # Preprocessing Data
	    self.doc = html.fromstring(pageContent) 
	
	    # Parse title
	    if self.__titleParser():
		# If title is valid, get status and decide whether continue parse
		if self.info['Status'] == "ACTIVE":
		    # If case is still ACTIVE, we don't need to parse it and put  
		    # them back into queue
		    self.__insertToPool(self.info['Complaint Number'])
		    #self.printInfo()
		
		elif self.info['Status'] == "RESOLVED":
		    # If case if RESOLVED, we need to parse all the infomation and 
		    # insert into database
		    self.__mainInfoParser()
		    self.__contentParser()
		    self.__insertToDB()
		    #self.printInfo()		

		self.__cleanUp()	
		    
		    

    def __getRawDataFromQueue(self):
	self.queueLock.acquire()
	rawData = self.JobQueue.get().read()
	self.queueLock.release()
	self.JobQueue.task_done()
	
	return rawData

    def __titleParser(self):
	title = self.doc.cssselect('title')
	if len(title) != 0:
	    m = PageAnalyzer.patternDict['title'].match(title[0].text_content())
	    if m == None:
		# If Not Match, there is some error, we need to log error and deal it
		self.__logError("title", title[0].text_content())
		return False
	    else:
		self.info['Complaint Number'] = m.group('complaint')
		self.info['Status'] = m.group('status')
		return True

	return False

    def __mainInfoParser(self):
	mainInfoList = self.doc.find_class('maininfo')
	mainInfoPatterns = PageAnalyzer.patternDict['mainInfoList']

	i = 0
	for item in mainInfoList:
	    if PageAnalyzer.mainInfoFieldList[i] in item.text_content():
		m = mainInfoPatterns[PageAnalyzer.mainInfoFieldList[i]] \
					    .match(item.text_content())
		if m == None:
		   self.__logError(PageAnalyzer.mainInfoFieldList[i], item) 
		else:
		    self.info[PageAnalyzer.mainInfoFieldList[i]] = m.group(2)
		
		i += 1


    def __contentParser(self):
	contentList = self.doc.find_class('content')
	contentPatterns = PageAnalyzer.patternDict['contentList']
    
	index = 0
	length = len(contentList)
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
		    else:
		        self.info[field] = m.group(2)
	    
	    index += 1
	
		
	

    def __insertToPool(self, number):
	# Insert ACTIVE case number back to pool
	pass
	

    def __cleanUp(self):
	self.info = copy.copy(PageAnalyzer.infoTemplate)
	self.doc = None

    def __logError(self, field, htmlNode):
	logging.error("Some error happens when parsing " 
	    + field 
	    + " for " 
	    + self.info['Complaint Number'] 
	    + ": " 
	    + htmlNode.text_content())

    def __insertToDB(self):
	# Insert current page info into DB
	self.__convertDatetime()
	dbm.put(self.info)

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

