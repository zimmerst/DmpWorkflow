"""
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
"""
import requests
from argparse import ArgumentParser
import os

from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL
# from DmpWorkflow.core import db
# from DmpWorkflow.core.models import Job, JobInstance, TYPES
# from DmpWorkflow.utils.db_helpers import parseJobXmlToDict

# _TYPES = list(TYPES) + ["NONE"]


def main(args=None):
    usage = "Usage: %(prog)s taskName xmlFile [options]"
    description = "create new instances for job in DB"
    parser = ArgumentParser(usage=usage, description=description)
    # parser.add_option("--instance", dest="inst",type=int, default = None,
    #                  help='use this to offset an instance')
    parser.add_argument("-n", "--name", help="task name", dest="name")
    parser.add_argument("-i", "--instances", help="number of instances", dest="inst", type=int)
    opts = parser.parse_args(args)
    # if len(sys.argv)!=3:
    #    print parser.print_help()
    #    raise Exception
    taskName = opts.name
    os.environ['DWF_JOBNAME'] = taskName
    ninst = opts.inst
    res = requests.post("%s/jobInstances/" % DAMPE_WORKFLOW_URL,
                        data={"taskname": taskName, "n_instances": ninst})
    res.raise_for_status()
    res = res.json()
    if res.json().get("result", "nok") == "ok":
        print 'Added %i instances'%int(ninst)
    else:
        print "Error message: %s" % res.json().get("error", "")

if __name__ == "__main__":
    main()

