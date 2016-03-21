'''
Created on Mar 15, 2016
@author: zimmer
@brief: base class for DAMPE Workflow
'''
from models import JobInstance

class DmpJob(object):
    def __init__(self,body):
        self.body = body
        
    def write_script(self,outfile):
        ''' based on meta-data should create job-executable '''
        pass

    def extract_xml_metadata(self,xmldoc):
        ''' given the structured job definition, read out and set variables '''
        pass
    
    def setInstanceParameters(self,JobInstance):
        ''' extract jobInstanceParameters to fully define job '''
        pass
    