'''
Created on Dec 16, 2014

@author: Wendong
'''
import logging.config
import os
import psutil

def rotateLogFile(filename, maxByte=8388608, rotate=5):
    if os.stat(filename).st_size < maxByte:
        return
    
    for pid in psutil.pids():
        p = psutil.Process(pid)
        if 'server.py' in p.cmdline():
            p.terminate()
            
    rt_fils = []
    for i in range(rotate):
        if not os.path.isfile(filename+'.bak'+str(i)):
            os.rename(filename, filename+'.bak'+str(i))
            return
        else:
            rt_fils.append((filename+'.bak'+str(i), os.stat(filename+'.bak'+str(i)).st_mtime))
    srt_fils = sorted(rt_fils, key=lambda x: x[1])
    # remove oldest log
    os.remove(srt_fils[0][0])
    os.rename(filename, srt_fils[0][0])
    
if os.path.isfile('app.log'):
    rotateLogFile('app.log')  
      
logging.config.fileConfig('logging.conf')
logger_r = logging.getLogger('root')
logger_c = logging.getLogger('crawler')
logger_p = logging.getLogger('proxy')

logger_analyzer = logging.getLogger('analyzer')
logger_db = logging.getLogger('db')
logger_query = logging.getLogger('query')

logger_prt = logging.getLogger('console')
