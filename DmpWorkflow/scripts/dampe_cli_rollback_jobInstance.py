"""
Created on Mar 15, 2016

@author: zimmer
"""
import requests
import json
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL
from DmpWorkflow.utils.tools import user_yes_no_query

def main(args=None):
    usage = "Usage: %(prog)s JobName [options]"
    description = "roll-back Jobs in DB"
    parser = ArgumentParser(usage=usage, description=description)
    parser.add_argument("--title", dest="title", type=str, default=None, help='name of job', required=True)
    parser.add_argument("--instance", dest="inst", type=int, default=None, help='to roll back specific instance', required=False)
    parser.add_argument("--status", dest="stat", type=str, default="Failed", help='jobs to be picked for roll-back', required=False)
    parser.add_argument("--n_min", dest="n_min", type=int, default=None, help='roll back everything above this number', required=False)
    parser.add_argument("--n_max", dest="n_max", type=int, default=None, help='roll back everything below this number', required=False)
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
    print 'found %i jobs that satisfy query conditions.'%len(jobs)
    user_yes_no_query("continue rolling back %i instances?"%len(jobs))
    for j in jobs:
        res = requests.post("%s/jobstatus/" % DAMPE_WORKFLOW_URL, data={"taskid": j['jobId'], "instanceid": j['instanceId'],
                                                                       "hostname": "None", "status": "New"})
        res.raise_for_status()
        res = res.json()
        if not res.get("result", "nok") == "ok":
            print ("error resetting instance %s"%res.get("error"))
    print 'rollback complete'
        


if __name__ == '__main__':
    main()
