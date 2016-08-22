"""
Created on Aug 17, 2016

@author: zimmer
@brief: retrieves the instance of a job and provides a 'lookup' of a given instance.
"""
from requests import get as r_get
from argparse import ArgumentParser
from os import environ
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL


def main(args=None):
    usage = "Usage: %(prog)s [options]"
    description = "lookup the instance of a job"
    parser = ArgumentParser(usage=usage, description=description)
    parser.add_argument("-n", "--name", help="task name", dest="name")
    parser.add_argument("-t", "--type", help="task type", dest="tasktype")
    parser.add_argument("-i", "--instId", help="instance", dest="inst", type=int)
    opts = parser.parse_args(args)
    res = r_get("%s/jobInstances/" % DAMPE_WORKFLOW_URL,
               data={"taskName": opts.name, "taskType": opts.tasktype, "instanceId": opts.inst})
    res.raise_for_status()
    res = res.json()
    if res.get("result", "nok") == "ok":
        inst = res.get("value")
        ## include some custom display below ##
        # .. do stuff here .. #
        #####
    else:
        print "Error message: %s" % res.get("error", "")

if __name__ == "__main__":
    main()
