"""
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
"""
from requests import post
from os import environ
from os.path import isfile
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, TYPES, SITES
from DmpWorkflow.utils.tools import parseJobXmlToDict


def createJob(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s taskName xmlFile [options]", description="create new job in DB")
    parser.add_argument("-t", "--type", dest="t_type", type=str, default="User", help='task type', choices=TYPES)
    parser.add_argument("--Ninstances", dest="Ninstances", type=int, default=0,
                        help='number of instances to create at the same time')
    parser.add_argument("-i", "--input", dest="xml", help="Path to job XML", required=True)
    parser.add_argument("-n", '--name', help="task Name", dest="name")
    parser.add_argument("-s", '--site', help="site to run at", dest="site", default='local', choices=SITES)
    parser.add_argument("-d", '--depends', help="depending tasks, separate by comma", dest="depends", default="")
    parser.add_argument("--set-var", dest="set_var", type=str, default=None,
                        help="set variables for streams, format is key1=value1;key2=value2, separate by ;")
    opts = parser.parse_args(args)
    override_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
    if opts.set_var is not None:
        var_dict = dict({tuple(val.split("=")) for val in opts.set_var.split(";")})
        override_dict['MetaData'] = [{"name": k, "value": v, "var_type": "string"} for k, v in var_dict.iteritems()]
    xmlFile = unicode(opts.xml)
    assert isfile(opts.xml), "must be an accessible file."
    n_instances = int(opts.Ninstances)
    xdict = parseJobXmlToDict(open(opts.xml, "r").read(),setVars=False)
    atts = xdict['atts']
    comment = xdict.get("comment",None)
    for key, value in vars(opts).iteritems():
        if key == 'set_var':
            continue
        if key == 't_type':
            key = "type"
        atts.setdefault(key, value)
    assert atts['site'] in SITES, "site not supported in DB %s" % atts['site']
    taskName = unicode(atts['name'])
    t_type = unicode(atts['type'])
    site = unicode(atts['site'])
    print atts
    dependent_tasks = opts.depends.split(",")
    data={"taskname": taskName, "t_type": t_type, "override_dict": str(override_dict),
          "n_instances": n_instances, "site": site,"depends": dependent_tasks}
    if comment is not None:
        data['comment']=comment
    res = post("%s/job/" % DAMPE_WORKFLOW_URL,data,files={"file": open(xmlFile, "rb")})
    res.raise_for_status()
    if res.json().get("result", "nok") == "ok":
        print 'Added job %s with %i instances' % (taskName, n_instances)
    else:
        print "Error message: %s" % res.json().get("error", "")

def createJobInstance(args=None):
    usage = "Usage: %(prog)s taskName xmlFile [options]"
    description = "create new instances for job in DB"
    parser = ArgumentParser(usage=usage, description=description)
    parser.add_argument("-n", "--name", help="task name", dest="name")
    parser.add_argument("-t", "--type", help="task type", dest="tasktype")
    parser.add_argument("-i", "--instances", help="number of instances", dest="inst", type=int)
    parser.add_argument("--instanceId",help="use this to offset the starting stream", dest="instanceId", type=int, default=0)
    parser.add_argument("--set-var", dest="set_var", type=str, default=None,
                        help="set variables for streams, format is key1=value1;key2=value2, separate by ;")
    opts = parser.parse_args(args)
    override_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
    if opts.set_var is not None:
        var_dict = dict({tuple(val.split("=")) for val in opts.set_var.split(";")})
        override_dict['MetaData'] = [{"name": k, "value": v, "var_type": "string"} for k, v in var_dict.iteritems()]
    taskName = opts.name
    environ['DWF_JOBNAME'] = taskName
    ninst = opts.inst
    res = post("%s/jobInstances/" % DAMPE_WORKFLOW_URL,
               data={"taskname": taskName, "tasktype": opts.tasktype, "n_instances": ninst, "instanceId": opts.instanceId,
                     "override_dict": str(override_dict)})
    res.raise_for_status()
    res = res.json()
    if res.get("result", "nok") == "ok":
        print 'Added %i instances to job %s' % (int(ninst), taskName)
    else:
        print "Error message: %s" % res.get("error", "")


if __name__ == "__main__":
    createJob()
