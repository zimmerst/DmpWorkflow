"""
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
"""
from requests import post
from argparse import ArgumentParser
from os import environ
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL


def main(args=None):
    usage = "Usage: %(prog)s taskName xmlFile [options]"
    description = "create new instances for job in DB"
    parser = ArgumentParser(usage=usage, description=description)
    parser.add_argument("-n", "--name", help="task name", dest="name")
    parser.add_argument("-t", "--type", help="task type", dest="tasktype")
    parser.add_argument("-i", "--instances", help="number of instances", dest="inst", type=int)
    parser.add_argument("--set-var",dest="set_var", type=str, default=None, help="set variables for streams, format is key1=value1;key2=value2, separate by ;")
    opts = parser.parse_args(args)
    override_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
    if opts.set_var is not None:
        var_dict = dict({tuple(val.split("=")) for val in opts.set_var.split(";")})
        override_dict['MetaData']=[{"name":k,"value":v,"type":str} for k,v in var_dict.iteritems()]
    taskName = opts.name
    environ['DWF_JOBNAME'] = taskName
    ninst = opts.inst
    res = post("%s/jobInstances/" % DAMPE_WORKFLOW_URL,
                        data={"taskname": taskName, "tasktype": 
                              opts.tasktype, "n_instances": ninst, "override_dict": str(override_dict)})
    res.raise_for_status()
    res = res.json()
    if res.get("result", "nok") == "ok":
        print 'Added %i instances to job %s'%(int(ninst), taskName)
    else:
        print "Error message: %s" % res.get("error", "")

if __name__ == "__main__":
    main()

