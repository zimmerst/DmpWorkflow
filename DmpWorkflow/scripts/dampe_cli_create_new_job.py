"""
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
"""
from requests import post
from os.path import isfile
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, TYPES, SITES
from DmpWorkflow.utils.tools import parseJobXmlToDict

def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s taskName xmlFile [options]", description="create new job in DB")
    parser.add_argument("-t", "--type", dest="t_type", type=str, default="User", help='task type', choices=TYPES)
    parser.add_argument("--Ninstances", dest="Ninstances", type=int, default=0,
                        help='number of instances to create at the same time')
    parser.add_argument("-i", "--input", dest="xml", help="Path to job XML", required=True)
    parser.add_argument("-n", '--name', help="task Name", dest="name")
    parser.add_argument("-s", '--site', help="site to run at", dest="site", default='local', choices=SITES)
    parser.add_argument("-d", '--depends', help="depending tasks, separate by comma", dest="depends", default="")
    parser.add_argument("--set-var",dest="set_var", type=str, default=None, help="set variables for streams, format is key1=value1;key2=value2, separate by ;")
    opts = parser.parse_args(args)
    override_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}
    if opts.set_var is not None:
        var_dict = dict({tuple(val.split("=")) for val in opts.set_var.split(";")})
        override_dict['MetaData']=[{"name":k,"value":v,"type":"str"} for k,v in var_dict.iteritems()]
    xmlFile = unicode(opts.xml)
    assert isfile(opts.xml), "must be an accessible file."
    n_instances = int(opts.Ninstances)
    xdict = parseJobXmlToDict(open(opts.xml,"r").read())
    atts = xdict['atts']
    for key, value in vars(opts).iteritems(): 
        if key == 'set_var': continue
        if key == 't_type': key = "type"
        atts.setdefault(key,value)
    assert atts['site'] in SITES, "site not supported in DB %s"%atts['site']
    taskName = unicode(atts['name'])
    t_type = unicode(atts['type'])
    site = unicode(atts['site'])
    print atts        
    dependent_tasks = opts.depends.split(",")
    res = post("%s/job/" % DAMPE_WORKFLOW_URL,
                        data={"taskname": taskName, "t_type": t_type, "override_dict": str(override_dict),
                              "n_instances": n_instances, "site" : site,
                              "depends":dependent_tasks},            
                        files={"file":open(xmlFile, "rb")})
    res.raise_for_status()
    if res.json().get("result", "nok") == "ok":
        print 'Added job %s with %i instances'%(taskName,n_instances)
    else:
        print "Error message: %s" % res.json().get("error", "")

if __name__ == "__main__":
    main()
