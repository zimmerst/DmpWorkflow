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
        self.instanceId = None
        self.InputFiles = []
        self.OutputFiles = []
        self.MetaData = []
        self.executable = ""
        self.exec_wrapper = ""
        self.__dict__.update(kwargs)
        self.extract_xml_metadata(job.body)
        
    def write_script(self,outfile):
        ''' based on meta-data should create job-executable '''
        pass
    
    def getJobName(self):
        return "-".join([self.jobId,self.instanceId])

    def extract_xml_metadata(self,xmldoc):
        ''' given the structured job definition, read out and set variables '''
        el = parseJobXmlToDict(xmldoc)
        self.InputFiles = el['InputFiles']
        self.OutputFiles = el['OutputFiles']
        self.MetaData = el['MetaData']
        self.exec_wrapper = el['script']
        self.executable = el['executable']
 
    def setInstanceParameters(self,JobInstance):
        ''' extract jobInstanceParameters to fully define job '''
        body = JobInstance.body
        keys = ['InputFiles','OutputFiles','MetaData']
        for key in keys:
            if key in body and isinstance(body[key],list):
                if len(body[key]):
                    self.__dict__[key]+=body[key]
        
    def submit(self):
        ''' handles the submission part '''
        pass
    
    def getStatus(self):
        ''' interacts with the backend HPC stuff and returns the status of the job '''
        pass
    
    def updateStatus(self,majorStatus,minorStatus):
        ''' passes status '''
        pass