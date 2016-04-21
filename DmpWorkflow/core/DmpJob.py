'''
Created on Mar 15, 2016
@author: zimmer
@brief: base class for DAMPE Workflow (HPC/client side)
'''
import os.path
import jsonpickle
import sys
import DmpWorkflow
from DmpWorkflow.core.models import JobInstance
from DmpWorkflow.utils.flask_helpers import parseJobXmlToDict, update_status
from DmpWorkflow.utils.tools import mkdir, touch, rm, Ndigits, safe_copy, exceptionHandler
from DmpWorkflow.hpc.lsf import LSF, BatchJob


# todo2: add cfg parsing variables.
class DmpJob(object):
    def __init__(self, job, **kwargs):
        self.wd = os.path.abspath(".")
        self.DBjob = job
        self.title = None
        self.jobId = str(job.id)
        self.instanceId = None
        self.batchId = None
        self.InputFiles = []
        self.OutputFiles = []
        self.MetaData = []
        self.type = None
        self.release = None
        self.logfile = None
        self.execCommand = None
        self.executable = ""
        self.exec_wrapper = ""
        self.script = None
        self.__dict__.update(kwargs)
        self.extract_xml_metadata(job.body)
        self.__updateEnv__()

    def __updateEnv__(self):
        for fi in self.InputFiles + self.OutputFiles + self.MetaData:
            for key in ['value', 'source', 'target']:
                if key in fi:
                    fi[key] = os.path.expandvars(fi[key])
        return

    def getJobName(self):
        return "-".join([self.jobId, self.instanceId])

    def extract_xml_metadata(self, xmldoc):
        """ given the structured job definition, read out and set variables """
        el = parseJobXmlToDict(xmldoc)
        self.InputFiles = el['InputFiles']
        self.OutputFiles = el['OutputFiles']
        self.MetaData = el['MetaData']
        self.exec_wrapper = el['script']
        self.executable = el['executable']
        self.__dict__.update(el['atts'])

    def setInstanceParameters(self, JobInstance):
        """ extract jobInstanceParameters to fully define job """
        body = JobInstance.body
        self.instanceId = JobInstance.instanceId  # aka stream
        keys = ['InputFiles', 'OutputFiles', 'MetaData']
        if isinstance(body, dict):
            for key in keys:
                if key in body and isinstance(body[key], list):
                    if len(body[key]):
                        self.__dict__[key] += body[key]

    def write_script(self, debug=False):
        """ based on meta-data should create job-executable """
        safe_copy(os.path.join(os.path.dirname(DmpWorkflow),"scripts/dampe_execute_payload.py"),
                  os.path.join(self.wd, "script.py"), debug=debug)
        json_file = open(os.path.join(self.wd, "job.json"), "wb")
        json_file.write(self.exportToJSON())
        json_file.close()
        scriptLOC = os.path.abspath(os.path.join(self.wd, "script.py"))
        jsonLOC = os.path.abspath(os.path.join(self.wd, "job.json"))
        cmd = "python %s %s" % (scriptLOC, jsonLOC)
        self.execCommand = cmd
        return

    def getSetupScript(self):
        return "${DAMPE_SW_DIR}/releases/DmpSoftware-%s/bin/thisdmpsw.sh" % self.release

    def createLogFile(self):
        mkdir(os.path.join("%s/logs" % self.wd))
        self.logfile = os.path.join("%s/logs" % self.wd, "%s.log" % self.getJobName())
        if os.path.isfile(self.logfile):
            rm(self.logfile)
        # create the logfile before submitting.
        touch(self.logfile)

    def updateStatus(self, majorStatus, minorStatus, **kwargs):
        """ passes status """
        update_status(self.jobId, self.instanceId, majorStatus, minor_status=minorStatus, **kwargs)

    def getStatusBatch(self):
        """ interacts with the backend HPC stuff and returns the status of the job """
        # todo: make the whole thing batch-independent!
        batch = LSF()
        ret = batch.status_map[batch.getJob(self.batchId, key="STAT")]
        return ret

    def submit(self, **kwargs):
        """ handles the submission part """
        self.createLogFile()
        bj = BatchJob(name=self.getJobName(), command=self.execCommand, logFile=self.logfile)
        bj.submit(**kwargs)
        self.batchId = bj.get("batchId")
        return self.batchId

    def exportToJSON(self):
        """ return a pickler of itself as JSON format """
        return jsonpickle.encode(self)

    def getSixDigits(self):
        return Ndigits(self.instanceId, 6)


def createFromJSON(jsonstr):
    dmpJob = jsonpickle.decode(jsonstr)
    return dmpJob
