'''
Created on Nov 12, 2014

@author: Wendong
'''

from multiprocessing import Queue
from core.crawler import CrawlerMaster
from core.proxy import ProxyManager
from time import time
from loggers import logger_r


if __name__ == '__main__':
    start = time()
    shared_queue = Queue()
    pm = ProxyManager(shared_queue)
    pm.start()
    cm = CrawlerMaster(shared_queue)
    cm.start()
    try:
        cm.join(timeout=28800)
    except Exception as e:
        logger_r.error('[Fatal] Process did not finish after 8 hours. ')
        logger_r.error(repr(e))
    pm.terminate()
    





