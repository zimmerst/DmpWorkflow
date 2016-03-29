'''
Created on Mar 15, 2016
@author: zimmer
@brief: base class for DAMPE Workflow
'''
from models import JobInstance

class DmpJob(object):
    def __init__(self,job,**kwargs):
        self.DBjob = job
        self.jobId = str(job.id)
        self.instanceId = None
        self.__dict__.update(kwargs)
    def write_script(self,outfile):
        ''' based on meta-data should create job-executable '''
        pass

    def extract_xml_metadata(self,xmldoc):
        ''' given the structured job definition, read out and set variables '''
        pass
    
    def setInstanceParameters(self,JobInstance):
        ''' extract jobInstanceParameters to fully define job '''
        pass
    
    def submit(self):
        ''' handles the submission part '''
        pass
    
    def getStatus(self):
        ''' interacts with the backend HPC stuff and returns the status of the job '''
        pass
    
    def updateStatus(self,majorStatus,minorStatus):
        ''' passes status '''
        pass