'''
Created on Nov 12, 2014

@author: Wendong

This module grabs a list of proxies from http://www.us-proxy.org/. It is 
used to provide different IPs for crawlers to avoid IP banning from the DOB 
server. 
'''

from threading import Thread, Lock
from multiprocessing import Process, Queue, JoinableQueue
from lxml import etree
from lxml.cssselect import CSSSelector
from time import sleep
from loggers import logger_p, logger_prt
from pony.orm import *
from datetime import date, timedelta
import requests, time, ConfigParser
from math import sqrt


db_p = Database()

class ProxyManager(Process):
    
    def __init__(self, shared_queue, reconn=30, timeout=90, update_freq=1200, pool_sz=20):
        Process.__init__(self)
        self._shared_queue = shared_queue
        self._timeout = timeout
        self._reconn = reconn
        self._update_freq = update_freq
        self._pool_sz = pool_sz
        self._task = JoinableQueue()
        self._output = Queue()
        self._broken = Queue()
        self.prx_list = []
        self.priority_list = [] 
        
    
    def run(self):        
        # initialize tester thread pool
        logger_prt.debug('Initializing proxy tester pool...')
        for _ in range(self._pool_sz):
            t = ProxyTester(self._task, self._output, self._broken)
            t.daemon = True
            t.start()
        
        conf = ConfigParser.RawConfigParser(allow_no_value=True)
        conf.read('db.conf')
        db_usr = conf.get('mysql', 'user')
        db_pwd = conf.get('mysql', 'passwd')
        db_p.bind('mysql', host='localhost', user=db_usr, passwd=db_pwd, db='DOB')    
        db_p.generate_mapping(check_tables=True, create_tables=True)
        db_p.create_tables()
        self._drop_old_backup()
            
        while True:
            if not self._get_us_proxy():
                self._get_backup()
            
            # wait for proxy testing
            self._task.join()
            logger_prt.info('proxy test finished')
            tmp_list = []
            while not self._output.empty():
                tmp_list.append(self._output.get())
            self.prx_list = sorted(tmp_list, key=lambda x: x[1])
            # prioritize the proxy list
            self._prioritize()
            self._shared_queue.put(self.priority_list[:])
            logger_prt.info('proxy list pushed')
            # back up available proxies
            self._backup()
            sleep(self._update_freq)
    
    
    def _get_us_proxy(self):
        trial = 0
        hold = 2
        finished = False
        while trial < self._reconn:
            trial = trial + 1
            try:
                res = requests.get('http://www.us-proxy.org', timeout=self._timeout)
                if res.status_code != 200:
                    logger_p.error('code ' + str(res.status_code) + ' error when connecting to http://www.us-proxy.org')
                elif self._parse_us_proxy(res.text):
                    finished = True
                    break
                else:
                    sleep(hold)
                    hold = hold * 2
                 
            except Exception as e:
                logger_p.error('failed to connect to http://www.us-proxy.org')
                logger_p.error(repr(e))
                sleep(hold)
                hold = hold * 2
        
        return finished
                
                
    def _parse_us_proxy(self, html_text):
        # parse ip and port from the html code
        sel = CSSSelector('#proxylisttable>tbody>tr')
        html = etree.HTML(html_text)
        tr = sel(html)
        if len(tr) == 0:
            logger_p.error('no proxy is found in the page, either the page source code is changed or page download error')
            return False
        
        # join the newly added proxies with the known proxies
        ip_set = set([p[0] for p in self.prx_list])      
        for t in tr:
            if (t[4].text == 'elite proxy' or t[4].text == 'anonymous'):
                ip_set.add('http://%s:%s' % (t[0].text, t[1].text))
        # put proxies into test task queue  
        for ip in ip_set:
            logger_prt.debug('Parsed proxy: ' + ip)
            self._task.put(ip)
        
        return True
    
    def _prioritize(self):
        '''
        Generate a priorty proxy list for random selection so that fast proxies will 
        have bigger chances to be selected.
        '''
        num = len(self.prx_list)
        if num == 0:
            self.priority_list = ['', '']
            return
        mean = 0.0
        for p in self.prx_list:
            mean += p[1]
        mean = mean / float(num)
        stdv = 0.0
        for p in self.prx_list:
            stdv += (p[1] - mean) ** 2
        stdv = sqrt(stdv / float(num))
#         tmp_list = ['', '', '', '']  # add two empty proxies for local IP request 
        tmp_list = []
        for p in self.prx_list:
            if p[1] <= mean - 0.5 * stdv:
                tmp_list.extend([p[0]] * 4)
            elif mean - 0.5 * stdv < p[1] < mean + stdv:
                tmp_list.extend([p[0]] * 2)
            else:
                tmp_list.extend([p[0]] * 1)
        self.priority_list = tmp_list
            
    @db_session
    def _get_backup(self):
        prxs = ProxyRecord.select()
        for p in prxs:
            logger_prt.debug('Retrieved backup proxy: ' + p.ip)
            self._task.put(p.ip)
    
    @db_session
    def _backup(self):
        for prx in self.prx_list:
            add_sql = '''INSERT INTO ProxyRecord (ip, check_date) VALUES ("%s", "%s")
                            ON DUPLICATE KEY UPDATE check_date="%s"''' \
                            % (prx[0], date.today().isoformat(), date.today().isoformat())
            db_p.execute(add_sql)
        commit()
            
    @db_session
    def _drop_old_backup(self):
        for p in ProxyRecord.select(lambda p: p.check_date < date.today() - timedelta(days=14)):
            p.delete()
            
    @db_session
    def _clean_backup(self):
        brk_list = []
        while not self._broken.empty():
            brk_list.append(self._broken.get())
        if len(brk_list) == 0:
            return
        else:
            dl_sql = 'DELETE FROM ProxyRecord where ip IN ("%s")' % '","'.join(brk_list)
            db_p.execute(dl_sql)
             
        
class ProxyTester(Thread):
    lock = Lock()
     
    def __init__(self, task_queue, output_queue, broken_queue):
        Thread.__init__(self)
        self._task = task_queue
        self._timeout = 90
        self._threshold = 1
        self._reconn = 4
        self._output = output_queue
        self._broken = broken_queue
         
         
    def run(self):
        while True:
            ip = self._task.get()
            logger_prt.debug('Testing proxy: ' + ip)
            proxy = {'http' : ip}
            ping = []
            for _ in range(self._reconn):
                try:
                    start = time.time()
                    res = requests.get('http://a810-bisweb.nyc.gov/bisweb/bispi00.jsp', proxies=proxy, timeout=self._timeout)
                    # If return code is not 200, then this proxy may not be working well.
                    # So we want to treat it as a timeout as well. 
                    if res.status_code != 200:
                        elapsed = self._timeout
                    else:
                        elapsed = time.time() - start
                    ping.append(elapsed)
                except Exception as e:
                    # any other exception will be treated as a timeout
                    ping.append(self._timeout)
                    
            avg_ping = 0.0
            for t in ping:
                avg_ping = avg_ping + t
            avg_ping = avg_ping / self._reconn
            # if the ping of a proxy is in acceptable range, then add it to the output
            if avg_ping < self._timeout * self._threshold:
                self._output.put([proxy['http'], avg_ping])
            else:
                self._broken.put(proxy['http'])
                 
            self._task.task_done()
            

class ProxyRecord(db_p.Entity):
    ip = PrimaryKey(unicode)
    check_date = Required(date)
                


