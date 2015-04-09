'''
Created on Nov 12, 2014

@author: Wendong
'''

from multiprocessing import Queue
from core.crawler import CrawlerMaster
from core.proxy import ProxyManager
from time import time
from loggers import logger_r
import traceback
import sys
import os


if __name__ == '__main__':
    try:
        shared_queue = Queue()
        pm = ProxyManager(shared_queue)
        pm.start()
        if len(sys.argv) == 2 and  sys.argv[1] == 'reset':
            cm = CrawlerMaster(shared_queue, reset=True)
        else:
            cm = CrawlerMaster(shared_queue)
        cm.start()
        try:
            cm.join(timeout=28800)
        except Exception as e:
            logger_r.error('[Fatal] Process did not finish after 8 hours. ')
            logger_r.error(repr(e))
        pm.terminate()
    except Exception as e:
        # logging fatal error
        logger_r.error(e)
        logger_r.error(traceback.format_exc())
        logger_r.error(sys.exc_info())
        
    





