'''
Created on Mar 15, 2016
@author: zimmer
@brief: base class for DAMPE Workflow (HPC/client side)
'''
from models import JobInstance
from utils.flask_helpers import parseJobXmlToDict

class DmpJob(object):
    def __init__(self,job,**kwargs):
        self.DBjob = job
        self.jobId = str(job.id)
        self.xml_data = self.extract_xml_metadata(job.body)
        self.instanceId = None
        self.inputFiles = []
        self.outputFiles = []
        self.__dict__.update(kwargs)
    
    def write_script(self,outfile):
        ''' based on meta-data should create job-executable '''
        pass
    
    def getJobName(self):
        return "-".join([self.jobId,self.instanceId])

    def extract_xml_metadata(self,xmldoc):
        ''' given the structured job definition, read out and set variables '''
        return parseJobXmlToDict(xmldoc)
    
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