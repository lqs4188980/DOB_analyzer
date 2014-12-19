'''
Created on Nov 12, 2014

@author: Wendong
'''

from multiprocessing import Queue
from core.crawler import CrawlerMaster
from core.proxy import ProxyManager
from time import time


if __name__ == '__main__':
    start = time()
    shared_queue = Queue()
    pm = ProxyManager(shared_queue)
    pm.start()
    cm = CrawlerMaster(shared_queue, reset=True)
    cm.start()
    cm.join()
    pm.terminate()
    





