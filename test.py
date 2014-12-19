'''
Created on Nov 21, 2014

@author: Wendong
'''

from pony.orm import *
from datetime import date, timedelta

db_p = Database('mysql', host='127.0.0.1', user='root', passwd='Bakemono', db='ProxyBackup')


class ProxyRecord(db_p.Entity):
    ip = PrimaryKey(unicode)
    check_date = Required(date)
                

@db_session
def func(prx_list):
    ds = 7
    for prx in prx_list:
        add_sql = '''INSERT INTO ProxyRecord (ip, check_date) VALUES ("%s", "%s")
                        ON DUPLICATE KEY UPDATE check_date="%s"''' \
                        % (prx[0], date.today().isoformat(), (date.today()- timedelta(days=ds)).isoformat())
        db_p.execute(add_sql)
        ds += 7

@db_session        
def getb():
    pp = ProxyRecord.select(lambda p: p.ip == "120e9103")
    for p in pp:
        p.delete()
        
    prxs = ProxyRecord.select()
    for p in prxs:
        print p.ip
        
@db_session
def cleanup():
    for p in ProxyRecord.select(lambda p: p.check_date < date.today() - timedelta(days=14)):
        p.delete()
    
            

db_p.generate_mapping(check_tables=True, create_tables=True)
db_p.create_tables()
prx_list = [['http://192.1.5.2', 25], ['http://225.1.5.2', 17], ['http://168.1.5.2', 30]]
func(prx_list)
cleanup()
getb()