'''
Created on Nov 21, 2014

@author: Wendong
'''

import ConfigParser

conf = ConfigParser.RawConfigParser(allow_no_value=True)
conf.read('dbconf.txt')

print conf.get('mysql', 'user')
print conf.get('mysql', 'passwd')