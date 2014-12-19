'''
Created on Dec 16, 2014

@author: Wendong
'''
import logging.config

logging.config.fileConfig('logging.conf')
logger_c = logging.getLogger('crawler')
logger_p = logging.getLogger('proxy')