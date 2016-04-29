"""
Created on Mar 15, 2016

@author: zimmer
"""
import requests
import json
import datetime
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
    parser.add_argument("--status", dest="stat", type=str, default="Any", 
                        help='jobs to be picked for roll-back, Any will return all statii!', required=False)
    parser.add_argument("--n_min", dest="n_min", type=int, default=None, help='roll back everything above this number', 
                        required=False)
    parser.add_argument("--n_max", dest="n_max", type=int, default=None, help='roll back everything below this number', 
                        required=False)
    opts = parser.parse_args(args)
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
                           "log": "", "cpu":None, "memory":None, "created_at":datetime.datetime.now()}
                res = requests.post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"args": json.dumps(my_dict)})
                res.raise_for_status()
                res = res.json()
                if not res.get("result", "nok") == "ok":
                    print ("error resetting instance %s"%res.get("error"))
            print 'rolled back %i instances'%len(jobs)
        else:
            print 'rollback aborted'
    else:
        print 'could not find any jobs satisfying the query.'


if __name__ == '__main__':
    main()
