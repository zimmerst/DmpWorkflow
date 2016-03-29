'''
Created on Mar 15, 2016

@author: zimmer
'''
from core import db
from core.models import *

def update_status(JobId,InstanceId,major_status,**kwargs):
    db.connect()
    my_job = Job.objects.filter(id=JobId)
    if not len(my_job):
        print 'could not find jobId %s'%JobId
        return
    my_job = my_job[0]
    assert major_status in MAJOR_STATII
    jInstance = my_job.getInstance(InstanceId)
    my_dict = {"status":major_status,"last_update":time.ctime()}
    my_dict.update(kwargs)
    for key,value in my_dict.iteritems():
        jInstance.__setattr__(key,value)
    #print 'calling my_job.save'
    my_job.save()
    return