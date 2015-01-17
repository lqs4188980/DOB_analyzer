'''
Created on Dec 16, 2014

@author: Wendong
'''
import logging.config

logging.config.fileConfig('logging.conf')
logger_r = logging.getLogger('root')
logger_c = logging.getLogger('crawler')
logger_p = logging.getLogger('proxy')

logger_analyzer = logging.getLogger('analyzer')
logger_db = logging.getLogger('db')
logger_query = logging.getLogger('query')

logger_prt = logging.getLogger('console')