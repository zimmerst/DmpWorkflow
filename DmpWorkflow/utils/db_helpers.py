"""
Created on Mar 15, 2016

@author: zimmer
"""
import logging
from time import ctime
from DmpWorkflow.core.models import Job, MAJOR_STATII, db
from argparse import ArgumentParser

log = logging.getLogger("core")

def update_status(JobId, InstanceId, major_status, **kwargs):
    ''' method to connect to db directly, without requests, i.e. should be run from server-side. '''
    db.connect()
    log.debug("calling update_status: %s %s status:%s",JobId,InstanceId,major_status)
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

def register_dataset(args):
    ''' convenience method to be called from Andrii's agent '''
    parser = ArgumentParser(usage="Usage: %(prog)s [options]", description="register dataset in dc")
    parser.add_argument("-s","--site",dest='site',type=str, default="UNIGE")
    parser.add_argument("-n","--dataset-name",dest='dname',type=str, default="", required=True)
    parser.add_argument("-N","--filename",dest='fname',type=str, default="", required=True)
    parser.add_argument("-p", "--xrd-prefix", dest="xrd_prefix", type = str, 
                        default="root://grid05.unige.ch:1094//dpm/unige.ch/home/dampe/", help='xrootd prefix')
    parser.add_argument("-t", "--filetype", dest="filetype", type = str, default="root", help='type')
    parser.add_argument("-S", "--setStatus", dest="status", type = str, default="New", help='site where the file is registered')
    parser.add_argument("-x", "--expandVars", dest="expandVars", action = 'store_true', default=False, help='if true, store absolute paths')
    parser.add_argument("-q", "--quiet", dest="quiet", action = 'store_true', default=False, help='if true, keep only essential information')
    parser.add_argument("-F", "--force", dest="force", action = 'store_true', default=False, help='if true, force overwriting existing file')
    parser.add_argument("-l", "--limit", dest="limit", type= int , default=100, help='limit list of entries returned')
    opts = parser.parse_args(args)
    db.connect()
    
    pass
