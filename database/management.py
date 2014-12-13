import os
import glob
import csv
import sys
from datetime import date, timedelta

import requests
from pony.orm import *
# from xlsxwriter.workbook import Workbook

from models import db, Complaint 
from models import ActiveCase
from query.handler import Query


class DBM: 
    """This module provide database access methods"""
    
    def __init__(self):
        self.initialize() 

    # putResolve and getResolve uses different dict keys with other method, 
    # which come from analyzer module
    @db_session
    def putResolve(self, info):
	if not exists(p for p in Complaint if p.complaint_number == \
						    info['Complaint Number']):
	    Complaint(
	        complaint_number = info['Complaint Number'],	
	        status = info['Status'],
	        bin = info['BIN'],
	        category = info['Category Code'],
	        lot = info['Lot'],
	        block = info['Block'],
	        zip = info['ZIP'],
	        received = info['Received'],
	        dob_violation = info['DOB Violation #'],
	        comments = info['Comments'],
	        owner = info['Owner'],
	        last_inspection = info['Last Inspection'],
	        borough = info['Borough'],
	        complaint_at = info['Complaint at'], 
	        ecb_violation = info["ECB Violation #s"]
	    )	

    @db_session
    def getResolve(self, complaint_number):
	complaint_number = str(complaint_number)
	info = {}
	if exists(p for p in Complaint if p.complaint_number == complaint_number):
	    obj = Complaint.get(complaint_number=complaint_number)
	    info = {
	        'Complaint Number': obj.complaint_number,
	        'Status': obj.status,
	        'BIN': obj.bin,
	        'Category Code': obj.category,
	        'Lot': obj.lot,
	        'Block': obj.block,
	        'ZIP': obj.zip,
	        'Received': obj.received,
	        'DOB Violation #': obj.dob_violation,
	        'Comments': obj.comments,
	        'Owner': obj.owner,
	        'Last Inspection': obj.last_inspection,
	        'Borough': obj.borough,
	        'Complaint at': obj.complaint_at, 
	        'ECB Violation #s': obj.ecb_violation, 
	    }
	return info

    @db_session
    def getAllResolve(self):
        objs = select(p for p in Complaint)[:]
        dictList = []
        for obj in objs:
            dictList.append({
	        'Complaint Number': obj.complaint_number,
	        'Status': obj.status,
	        'BIN': obj.bin,
	        'Category Code': obj.category,
	        'Lot': obj.lot,
	        'Block': obj.block,
	        'ZIP': obj.zip,
	        'Received': obj.received,
	        'DOB Violation #': obj.dob_violation,
	        'Comments': obj.comments,
	        'Owner': obj.owner,
	        'Last Inspection': obj.last_inspection,
	        'Borough': obj.borough,
	        'Complaint at': obj.complaint_at, 
	        'ECB Violation #s': obj.ecb_violation,  
            })
        return dictList

    @db_session
    def putActive(self, activeInfo):
	# This method is not for the active case that I parsed out
	if not exists(p for p in ActiveCase if p.complaint_number == \
					activeInfo["complaint_number"]):
	    ActiveCase(
	        complaint_number = activeInfo["complaint_number"], 
	        date_entered = activeInfo["date_entered"]
	    )

    @db_session
    def getActive(self, complaintnum):
	info = {}
        complaintnum = str(complaintnum)
	if exists(p for p in ActiveCase if p.complaint_number == complaintnum):
	    obj = ActiveCase.get(complaint_number=complaintnum)	

	    info['complaint_number'] = obj.complaint_number
	    info['date_entered'] = obj.date_entered

	return info	

    def initialize(self):
	db.bind('mysql', host='localhost', user='root', passwd='', db='DOB')
	db.generate_mapping(check_tables=True, create_tables=True)

    # @db_session
    # def export(self):
    #     db.select('* FROM complaint INTO OUTFILE \'output.csv\' FIELDS TERMINATED BY \',\'')
    #     for csvfile in glob.glob(os.path.join('.', '*.csv')):
    #         workbook = Workbook(csvfile + '.xlsx')
    #         worksheet = workbook.add_worksheet()
    #         with open(csvfile, 'rb') as f:
    # 	        reader = csv.reader(f)
    # 	        for r, row in enumerate(reader):
    # 	            for c, col in enumerate(row):
    # 	                worksheet.write(r, c, col)
    # 	    workbook.close()

    @db_session
    def getNewClosedCase(self):
	q = Query("https://data.cityofnewyork.us/resource/eabe-havv.json")
	newClose = q.getAllClosedCaseSet()

	# Subtract all resolved complaint number
	#print select(p.complaint_number for p in Complaint)[:]
	newClose -= set(select(p.complaint_number for p in Complaint)[:])

	return newClose

    @db_session
    def getNDayActiveCase(self, nday):
	nday = timedelta(days=int(nday))
	return select(p.complaint_number for p in ActiveCase \
			    if p.date_entered >= (date.today() - nday) and \
				p.date_entered < date.today())[:]

    @db_session
    def getRecentActiveCaseNum(self, start, end):
	start = str(start)
	end = str(end)
	base = select(p for p in ActiveCase \
			    if p.complaint_number > start and \
				p.complaint_number < end)
	if base.count() == 0:
	    return None
	else:
	    return base.order_by(desc(ActiveCase.complaint_number)).limit(1)[0].complaint_number
