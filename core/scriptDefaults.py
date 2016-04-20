'''
Created on Apr 20, 2016

@author: zimmer
@brief: prototype script that handles config parsing etc.

'''
import ConfigParser, os

cfg = ConfigParser.SafeConfigParser()
cfg.read(os.getenv("WorkflowConfig","config/dampe.cfg"))
