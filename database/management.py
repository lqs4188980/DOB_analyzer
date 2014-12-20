import os
import glob
import csv
import sys
import datetime
import logging
import threading
import ConfigParser
from datetime import date, timedelta, datetime

import requests
from pony.orm import *
# from xlsxwriter.workbook import Workbook

from models import db, Complaint 
from models import Warehouse
from query.handler import Query
from loggers import logger_db


class DBM: 
    """This module provide database access methods"""
    _isInitialized = False
    _lock = threading.Lock()
    def __init__(self):
        if not DBM._isInitialized:
            DBM._lock.acquire()
            self.initialize() 
            DBM._isInitialized = True
            DBM._lock.release()

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
                ecb_violation = info["ECB Violation #"]
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
                'ECB Violation #': obj.ecb_violation, 
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
                'ECB Violation #': obj.ecb_violation,  
            })
        return dictList

    @db_session
    def getResolveByInspectionDateRange(self, startDate, endDate):
        # This method provide query functions that enable query for a date range
        # if startDate and endDate are not instance of date object, return empty list
        # Return an iterable list sorted by date in descending order
        if not isinstance(startDate, datetime) or not isinstance(endDate, datetime):
            logger_db.error("DB Error@getResolveByDateRange: \
                                    False argument type; expected datetime type")
            return []
        
        results = []
        objs = select(p for p in Complaint if p.last_inspection >= startDate and \
                                p.last_inspection <= endDate)
        for obj in objs:
            results.append(self.__resolveDataPacker(obj))

        return results

    @db_session
    def getResolveByCategoryCode(self, category):
        results = []
        objs = select(p for p in Complaint if p.category == category)
        for obj in objs:
            results.append(self.__resolveDataPacker(obj))
    
        return results

    @db_session
    def putWarehouseCase(self, caseInfo):
        if not exists(p for p in Warehouse if p.complaint_number == \
                                        caseInfo["Complaint Number"]):
            Warehouse(
                complaint_number = caseInfo["Complaint Number"], 
                date_entered = date.today(),
                status = caseInfo["Status"]
            )
        else:
            obj = get(p for p in Warehouse if p.complaint_number == \
                                        caseInfo["Complaint Number"])
            obj.status = caseInfo["Status"]

    @db_session
    def getWarehouseCase(self, complaintnum):
        info = {}
        complaintnum = str(complaintnum)
        if exists(p for p in Warehouse if p.complaint_number == complaintnum):
            obj = Warehouse.get(complaint_number=complaintnum)        

            info['Complaint Number'] = obj.complaint_number
            info['Date Entered'] = obj.date_entered
            info['Status'] = obj.status

        return info        

    @db_session
    def deleteWarehouseCase(self, complaintnum):
        complaintnum = str(complaintnum)
        obj = select(p for p in Warehouse if p.complaint_number == complaintnum)
        if len(obj) == 1:
            obj.get().delete()

    def initialize(self):
        conf = ConfigParser.RawConfigParser(allow_no_value=True)
        conf.read('db.conf')
        db_usr = conf.get('mysql', 'user')
        db_pwd = conf.get('mysql', 'passwd')
        db.bind('mysql', host='localhost', user=db_usr, passwd=db_pwd, db='DOB')
        db.generate_mapping(check_tables=True, create_tables=True)

    # @db_session
    # def export(self):
    #     db.select('* FROM complaint INTO OUTFILE \'output.csv\' FIELDS TERMINATED BY \',\'')
    #     for csvfile in glob.glob(os.path.join('.', '*.csv')):
    #         workbook = Workbook(csvfile + '.xlsx')
    #         worksheet = workbook.add_worksheet()
    #         with open(csvfile, 'rb') as f:
    #                 reader = csv.reader(f)
    #                 for r, row in enumerate(reader):
    #                     for c, col in enumerate(row):
    #                         worksheet.write(r, c, col)
    #             workbook.close()

    @db_session
    def getNewClosedCaseFromADate(self, startDate=None):
        # startDate should be an instance of datetime
        # Default is NoneType, which lead to return all new closed case
        q = Query("https://data.cityofnewyork.us/resource/eabe-havv.json")
        newClose = q.getAllClosedCaseSet(startDate)

        # Subtract all resolved complaint number
        #print select(p.complaint_number for p in Complaint)[:]
        newClose -= set(select(p.complaint_number for p in Complaint)[:])
        newClose -= set(select(p.complaint_number for p in Warehouse if p.status == "CLOSED")[:])

        return newClose

    @db_session
    def getNDayActiveCase(self, nday):
        nday = timedelta(days=int(nday))
        return select(p.complaint_number for p in Warehouse \
                            if p.status == "ACTIVE" and \
                                p.date_entered >= (date.today() - nday) and \
                                p.date_entered <= date.today())[:]

    @db_session
    def getRecentActiveCaseNum(self, start, end):
        start = str(start)
        end = str(end)
        base = select(p for p in Warehouse \
                            if p.status == "ACTIVE" and \
                                p.complaint_number > start and \
                                p.complaint_number < end)
        if base.count() == 0:
            return None
        else:
            return base.order_by(desc(Warehouse.complaint_number))\
                                                        .limit(1)[0].complaint_number


    def __resolveDataPacker(self, resolve):
        return {
            'Complaint Number': resolve.complaint_number,
            'Status': resolve.status,
            'BIN': resolve.bin,
            'Category Code': resolve.category,
            'Lot': resolve.lot,
            'Block': resolve.block,
            'ZIP': resolve.zip,
            'Received': resolve.received,
            'DOB Violation #': resolve.dob_violation,
            'Comments': resolve.comments,
            'Owner': resolve.owner,
            'Last Inspection': resolve.last_inspection,
            'Borough': resolve.borough,
            'Complaint at': resolve.complaint_at, 
            'ECB Violation #': resolve.ecb_violation,   
        }
