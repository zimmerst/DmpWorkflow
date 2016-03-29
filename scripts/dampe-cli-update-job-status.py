'''
Created on Mar 15, 2016

@author: zimmer
'''
import copy, sys, time
from core import db
from core.models import MAJOR_STATII, Job
from core.DmpJob import DmpJob

# each job has an immutable document identifier.
# jobId instanceId major_status minor_status

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
    
if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser()
    usage = "Usage: %prog JobID InstanceID status [options]"
    description = "update job in DB"
    parser.set_usage(usage)
    parser.set_description(description)
    parser.add_option("--minor_status", dest="minor_status",type=str, default = None,help='minor status')
    parser.add_option("--hostname",     dest="hostname",    type=str, default = None,help='hostname'    )
    parser.add_option("--batchId",      dest="batchId",     type=str, default = None,help='batchId'     )
    (opts, arguments) = parser.parse_args()
    #if len(sys.argv)!=3:
    #    print parser.print_help()
    #    raise Exception
    JobId           = sys.argv[1]
    InstanceId      = sys.argv[2]
    major_status    = sys.argv[3]

    my_dict = {}
    for key in opts.__dict__:
        if not opts.__dict__[key] is None:
            my_dict[key]=opts.__dict__[key]
    
    update_status(JobId,InstanceId,str(major_status),**my_dict)