'''
Created on Mar 15, 2016

@author: zimmer
'''
from utils.scriptDefaults import cfg
import copy, sys, time
from utils.flask_helpers import update_status
# each job has an immutable document identifier.
# jobId instanceId major_status minor_status
    
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