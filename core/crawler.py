'''
Created on Nov 12, 2014

@author: Wendong


'''

from threading import Thread, Lock
from multiprocessing import Process, Queue, JoinableQueue
from lxml import etree
from lxml.cssselect import CSSSelector
from time import sleep
from datetime import datetime
from database.management import DBM
from query.handler import Query
from analyzer import PageAnalyzer
from loggers import logger_c, logger_prt
import random
import requests
import sys, traceback




MAX_CRAWL_ERROR = 32

class CrawlerMaster(Process):
    
    def __init__(self, proxy_queue, pool_sz=75, reset=False, init_date=datetime(2014,12,1)):
        Process.__init__(self)
        self._proxy_queue = proxy_queue
        self._pool_sz = pool_sz
        self._reset = reset
        self._init_date = init_date
        self._task = JoinableQueue()
        self._output = JoinableQueue()
        self._error_case = {}
        self._lock = None
        self._proxies = ['', ''] # add two empty proxies for local IP requests
        
    def set_proxy_queue(self, proxy_queue):
        self._proxy_queue = proxy_queue
        
    def run(self):
        self._lock =  Lock()   
        
        # schedule the daily tasks
        if self._reset:
            start_nums = self._first_schedule()
        else:
            start_nums = self._daily_schedule()
        
        # wait for the first proxy list for 10 minutes
        # if it is not ready after 10 minutes, we will proceed
        # and use the local IP for request task
        try:
            self._proxies = self._proxy_queue.get(timeout=3600)
        except Exception as e:
            logger_c.warning(e)
            logger_c.warning("Warning: CrawlerMaster did not get the proxy list after 10 minutes. Proceed by using local IP requests.")
        # spawn a proxy updater thread to get newest proxies
        prx_updater = ProxyUpdater(self._proxy_queue, self._proxies, self._lock)
        prx_updater.daemon = True
        prx_updater.start()
         
        logger_prt.debug("Searching for the latest id...")
        search_threads = []
        latest_queue = Queue()
        for n in start_nums:
            t = LatestCaseFinder(n, latest_queue, self._proxies, self._lock)
            t.daemon = True
            t.start()  
            search_threads.append(t)
        for t in search_threads:
            t.join()
           
        end_nums = []
        while not latest_queue.empty():
            end_nums.append(latest_queue.get())
        for i,j in zip(start_nums, sorted(end_nums)):
            for n in range(i, j+1):
                task = {'id':str(n), \
                        'url':'http://a810-bisweb.nyc.gov/bisweb/OverviewForComplaintServlet?complaintno='+ str(n) +'&requestid=0'}
                logger_prt.debug('Adding ' + str(task['id']))
                self._task.put(task)
                
        
        # dispatching the daily tasks
        # initialize crawler thread pool
        logger_prt.debug('Number of available proxies: ' + str(len(self._proxies)))
        logger_prt.debug('Initializing crawler thread pool...')
        for _ in range(self._pool_sz):
            t = Crawler(self._task, self._output, self._proxies, self._lock, self._error_case)
            t.daemon = True
            t.start()
        
        logger_prt.debug("Initializing analyzer thread pool...")
        for _ in range(5):
            t = PageAnalyzer(self._output, self._task)
            t.daemon = True
            t.start()
            
        self._task.join()
        logger_prt.debug("Daily tasks done!")
        
        
        
    def _first_schedule(self):
        dbm = DBM()
        case_set = dbm.getNewClosedCaseFromADate(self._init_date)
        for i in case_set:
            if int(i) < 2000000 or int(i) > 4999999:
                continue
            task = {'id':i, \
                    'url':'http://a810-bisweb.nyc.gov/bisweb/OverviewForComplaintServlet?complaintno='+ str(i) +'&requestid=0'}
            logger_prt.debug('Adding ' + str(task['id']))
            self._task.put(task)
        nyc_open = Query()
        start_nums = []
        n2 = nyc_open.getRecentActiveCaseNum(2000000, 2999999)
        n3 = nyc_open.getRecentActiveCaseNum(3000000, 3999999)
        n4 = nyc_open.getRecentActiveCaseNum(4000000, 4999999)
        start_nums.append(int(n2 if n2 else 0))
        start_nums.append(int(n3 if n3 else 0))
        start_nums.append(int(n4 if n4 else 0))
        return start_nums
    
    def _daily_schedule(self):
        dbm = DBM()
        case_set = set(dbm.getNDayActiveCase(3))
        case_set.update(dbm.getNewClosedCaseFromADate(self._init_date))
        for i in case_set:
            if int(i) < 2000000 or int(i) > 4999999:
                continue
            task = {'id':i, \
                    'url':'http://a810-bisweb.nyc.gov/bisweb/OverviewForComplaintServlet?complaintno='+ str(i) +'&requestid=0'}
            self._task.put(task)
            logger_prt.debug('Adding ' + str(task['id']))
        nyc_open = Query()
        start_nums = []
        r2 = dbm.getRecentActiveCaseNum(2000000, 2999999)
        r3 = dbm.getRecentActiveCaseNum(3000000, 3999999)
        r4 = dbm.getRecentActiveCaseNum(4000000, 4999999)
        n2 = nyc_open.getRecentActiveCaseNum(2000000, 2999999)
        n3 = nyc_open.getRecentActiveCaseNum(3000000, 3999999)
        n4 = nyc_open.getRecentActiveCaseNum(4000000, 4999999)
        start_nums.append(max( int(r2 if r2 else 0), int(n2 if n2 else 0) ))
        start_nums.append(max( int(r3 if r3 else 0), int(n3 if n3 else 0) ))
        start_nums.append(max( int(r4 if r4 else 0), int(n4 if n4 else 0) ))
        return start_nums
        
    
class ProxyUpdater(Thread):
    
    def __init__(self, proxy_queue, proxies, lock):
        Thread.__init__(self)
        self._proxy_queue = proxy_queue
        self._proxies = proxies
        self._lock = lock
        
    def run(self):
        while True:
            prx_list = self._proxy_queue.get()
            logger_prt.info("%d proxies get" % prx_list)
            self._lock.acquire()
            del self._proxies[:]
            self._proxies.extend(prx_list)
            self._lock.release()
            logger_prt.info("Proxy list updated!")


class Crawler(Thread):
    
    def __init__(self, task, output, proxies, lock, error_case, reconn=4, timeout=120):
        Thread.__init__(self)
        self._task = task
        self._output = output
        self._proxies = proxies
        self._lock = lock
        self._error_case = error_case
        self._reconn = reconn
        self._timeout = timeout
    
    def run(self):
        while True:
            try:
                task = self._task.get()
                if 'error' in task:
                    if task['id'] in self._error_case:
                        self._error_case[task['id']] += 1
                        if self._error_case[task['id']] > MAX_CRAWL_ERROR:
                            logger_c.error('downloading case-' + str(task['id']) + ' exceeds the max number of re-try')
                            self._task.task_done()
                            logger_prt.debug('Task done ' + str(task["id"]))
                            continue
                    else:
                        self._error_case.update({task['id']:1})
                self._lock.acquire()
                proxy = random.choice(self._proxies)                
                self._lock.release()
                logger_prt.debug("Fetching " + str(task['id']) + " via " + proxy)
                hold = 2
                for _ in range(self._reconn):
                    try:
                        res = requests.get(task['url'], proxies={'http':proxy}, timeout=self._timeout)
                        if res.status_code == 200:
                            task.update({'text':res.text})
                            self._output.put(task)
                            break
                        else:
                            logger_c.error('code ' + str(res.status_code) + ' error when connecting to ' + task['url'])
                            sleep(hold)
                            hold *= 2
                    except Exception as e:
                        logger_c.error(repr(e))
                        sleep(hold)
                        hold *= 2
                else:
                    logger_c.error('case-'+str(task['id'])+' download failure after 4 re-trys.')
                    task.update({'error':'download error'})
                    self._task.put(task)
                    self._task.task_done()
                    logger_prt.debug('Task done ' + str(task['id']))
                    logger_prt.debug('Adding ' + str(task['id']))
            except Exception as e:
                logger_c.error("Critical error, Crawler thread failed.")
                logger_c.error(repr(e))
                logger_c.error(traceback.format_exc())
                logger_c.error(sys.exc_info())


class LatestCaseFinder(Thread):
    ''' This thread finds the latest active case number with a given starting number. '''
    lock = Lock()
    
    def __init__(self, start, output, proxies, lock):
        Thread.__init__(self)
        self._start = start
        self._step = 512
        self._timeout = 120
        self._output = output
        self._proxies = proxies
        self._lock = lock
        
    def run(self):
        start = self._start
        step = self._step
        
        # determine the search range
        while self._valid(start + step):
            step = step * 2
        # use a binary search method to find out the latest case number
        while step != 1:
            last = step
            if self._valid(start + last / 2):
                start = start + last / 2
                step = last - last / 2
            else:
                step = last / 2        
        if self._valid(start + 1):
            self._output.put(start + 1)
        else:
            self._output.put(start)
    
    
    def _valid(self, num):
        ''' This method tests whether a given case number exists or not '''
        url = 'http://a810-bisweb.nyc.gov/bisweb/OverviewForComplaintServlet?complaintno='+ str(num) +'&requestid=0'
        while True:
            try:
                sleep(2)
                self._lock.acquire()
                proxy = random.choice(self._proxies)
                self._lock.release()
                logger_prt.debug("Trying id " + str(num) + ' @ ' + proxy)
                res = requests.get(url, timeout=self._timeout, proxies={'http':proxy})
                if res.status_code != 200:
                    logger_c.error("status code " + str(res.status_code) + " error")
                    continue
                else:
                    # try to find returned error message in the page
                    sel = CSSSelector('.errormsg')
                    html = etree.HTML(res.text)
                    td = sel(html)
                    # test if this case number exists
                    if (len(td) != 0):
                        if ('COMPLAINT NOT FOUND' in td[0].text):
                            return False
                        # catch error other than COMPLAINT NOT FOUND
                        else:
                            logger_c.error('Uncatched case ' + str(num) + 'request error. ' + td[0].text)
                            continue
                    # if the case number exists, return true
                    else:
                        return True
            except Exception as e:
                logger_c.error(repr(e))
                continue
