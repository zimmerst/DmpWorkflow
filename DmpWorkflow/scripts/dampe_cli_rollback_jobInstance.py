"""
Created on Mar 15, 2016

@author: zimmer
"""
import requests
import json
import sys
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL
from DmpWorkflow.utils.tools import query_yes_no

def main(args=None):
    usage = "Usage: %(prog)s JobName [options]"
    description = "roll-back Jobs in DB"
    parser = ArgumentParser(usage=usage, description=description)
    parser.add_argument("--title", dest="title", type=str, default=None, help='name of job, required!', required=True)
    parser.add_argument("--instance", dest="inst", type=int, default=None, 
                        help='to roll back specific instance', required=False)
    parser.add_argument("--status", dest="stat", type=str, default="Failed", 
                        help='jobs to be picked for roll-back, Any will return all statii!', required=False)
    parser.add_argument("--n_min", dest="n_min", type=int, default=None, help='roll back everything above this number', 
                        required=False)
    parser.add_argument("--n_max", dest="n_max", type=int, default=None, help='roll back everything below this number', 
                        required=False)
    opts = parser.parse_args(args)
    if opts.n_min is None and opts.n_max is None and opts.inst is None and opts.stat == "Any":
        q = query_yes_no("WARNING: you are requesting to roll back all instances of job %s.\
                        \nThis query may take a while to be completed, are you sure?"%opts.title)
        if not q:
            print 'rollback aborted'
            sys.exit()
    if not (opts.n_min is None and opts.n_max is None):
        _range = opts.n_max - opts.n_min
        if _range > 100: print 'WARNING: you are querying more than 100 jobs, this may take a while to complete'
    my_dict = {}
    for key in opts.__dict__:
        if opts.__dict__[key] is not None:
            my_dict[key] = opts.__dict__[key]
    # get all jobs to roll back.
    res = requests.get("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data=my_dict)
    res.raise_for_status()
    res = res.json()
    if res.get("result","nok") != "ok":
        print "error %s" % res.get("error")
    jobs = res.get("jobs")
    if len(jobs):
        print 'found %i jobs that satisfy query conditions.'%len(jobs)
        if query_yes_no("continue rolling back %i instances?"%len(jobs)):
            for j in jobs:
                my_dict = {"t_id": j['jobId'], "inst_id": j['instanceId'], 
                           "major_status": "New", "minor_status":"AwaitingBatchSubmission", "hostname":None,
                           "batchId":None, "status_history":[], 
                           "log": "", "cpu":[], "memory":[], "created_at":"Now"}
                res = requests.post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args": json.dumps(my_dict)})
                res.raise_for_status()
                res = res.json()
                if not res.get("result", "nok") == "ok":
                    print ("error resetting instance %s"%res.get("error"))
            print 'rolled back %i instances'%len(jobs)
        else:
            print 'rollback aborted'
            sys.exit()
    else:
        print 'could not find any jobs satisfying the query.'
        sys.exit()

if __name__ == '__main__':
    main()
