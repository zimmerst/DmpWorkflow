"""
Created on Mar 15, 2016

@author: zimmer
"""
import time
import logging
from DmpWorkflow.core import db
from DmpWorkflow.core.models import Job, MAJOR_STATII

log = logging.getLogger("core")

def update_status(JobId, InstanceId, major_status, **kwargs):
    ''' method to connect to db directly, without requests, i.e. should be run from server-side. '''
    db.connect()
    log.debug("calling update_status: %s %s status:%s"%(JobId,InstanceId,major_status))
    my_job = Job.objects.filter(id=str(JobId))
    if not len(my_job):
        log.exception("update_status: could not find jobId")
        return
    my_job = my_job[0]
    assert major_status in MAJOR_STATII
    jInstance = my_job.getInstance(InstanceId)
    # print jInstance
    my_dict = {"last_update": time.ctime()}
    my_dict.update(kwargs)
    for key, value in my_dict.iteritems():
        jInstance.__setattr__(key, value)
    # finally, update status
    jInstance.setStatus(major_status)
    # print 'calling my_job.save'
    my_job.update()
    log.debug("updated job")
    return


