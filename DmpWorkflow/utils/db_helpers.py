"""
Created on Mar 15, 2016

@author: zimmer
"""
import time

from DmpWorkflow.core import db
from DmpWorkflow.core.models import Job, MAJOR_STATII


def update_status(JobId, InstanceId, major_status, **kwargs):
    ''' method to connect to db directly, without requests, i.e. should be run from server-side. '''
    db.connect()
    my_job = Job.objects.filter(id=JobId)
    if not len(my_job):
        print 'could not find jobId %s' % JobId
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
    return


