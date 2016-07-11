"""
Created on Mar 15, 2016

@author: zimmer
"""
import logging
from time import ctime
from datetime import datetime
from DmpWorkflow.core.models import Job, MAJOR_STATII, db
from DmpWorkflow.core.datacat import DataSet, DataFile, DataReplica
from os.path import basename, splitext, dirname

log = logging.getLogger("core")


def update_status(JobId, InstanceId, major_status, **kwargs):
    """ method to connect to db directly, without requests, i.e. should be run from server-side. """
    db.connect()
    log.debug("calling update_status: %s %s status:%s", JobId, InstanceId, major_status)
    my_job = Job.objects.filter(id=str(JobId))
    if not len(my_job):
        log.exception("update_status: could not find jobId")
        return
    my_job = my_job[0]
    assert major_status in MAJOR_STATII
    jInstance = my_job.getInstance(InstanceId)
    # print jInstance
    my_dict = {"last_update": ctime()}
    my_dict.update(kwargs)
    for key, value in my_dict.iteritems():
        jInstance.__setattr__(key, value)
    # finally, update status
    jInstance.setStatus(major_status)
    # print 'calling my_job.save'
    my_job.update()
    log.debug("updated job")
    return

def register_dataset(**kwargs):
    defaultTime = "19000101000000"
    prefix = kwargs.get("prefix","root://grid05.unige.ch:1094//dpm/unige.ch/home/dampe")
    FileName = kwargs.get("FileName",None)
    if not FileName: 
        log.error("must provide at least a file name!")
        return
    TStart = datetime.strptime(kwargs.get("TStart",defaultTime),"%Y%m%d%H%M%S")
    TStop =  datetime.strptime(kwargs.get("TStop",defaultTime),"%Y%m%d%H%M%S")
    Gti   = float(kwargs.get("Gti",0.))
    FileType= kwargs.get("FileType","root")
    DataType= kwargs.get("DataType","USR")
    DataClass=kwargs.get("DataClass","None")
    Release  =kwargs.get("Release","None")
    # try to guess dataset name
    pure_file_name = basename(FileName)
    DataFileName, FileType = splitext(pure_file_name)
    DataSetName = dirname(FileName.replace(prefix,""))
    if DataSetName.startswith("/"): 
        DataSetName = DataSetName.split("/")[1]
    else: DataSetName.split("/")[0]
    