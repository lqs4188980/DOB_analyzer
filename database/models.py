import datetime

from pony.orm import *

"""This module describe database entities"""

db = Database()            # Use late binding for better compatibility

class Complaint(db.Entity):
    complaint_number = PrimaryKey(str, auto=False)
    status = Required(str)
    bin = Optional(str)
    category = Optional(str)
    lot = Optional(str)
    block = Optional(str)
    zip = Optional(str)
    received = Optional(datetime.date)
    dob_violation = Optional(str)
    comments = Optional(str, 512)
    owner = Optional(str)
    last_inspection = Optional(datetime.date)
    disposition = Optional(str)
    borough = Optional(str)
    complaint_at = Optional(str)
    ecb_violation = Optional(str)
    
class Warehouse(db.Entity):
    complaint_number = PrimaryKey(str, auto=False)
    date_entered = Required(datetime.date)
    status = Required(str)

#class ClosedCase(db.Entity):
#    complaint_number = PrimaryKey(str, auto=False)
