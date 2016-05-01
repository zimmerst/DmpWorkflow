"""
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
"""
import requests
from os.path import isfile
import os.path as oP
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, TYPES, SITES
from DmpWorkflow.utils.tools import parseJobXmlToDict

def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s taskName xmlFile [options]", description="create new job in DB")
    parser.add_argument("-t", "--type", dest="t_type", type=str, default="User", help='task type', choices=TYPES)
    parser.add_argument("--Ninstances", dest="Ninstances", type=int, default=0,
                        help='number of instances to create at the same time')
    parser.add_argument("-i", "--input", dest="xml", help="Path to job XML", required=True)
    parser.add_argument("-n", '--name', help="task Name", dest="tname")
    parser.add_argument("-s", '--site', help="site to run at", dest="site", default='local', choices=SITES)
    opts = parser.parse_args(args)
    xmlFile = unicode(opts.xml)
    assert isfile(opts.xml), "must be an accessible file."
    xdict = parseJobXmlToDict(open(opts.xml,"r").read())
    atts = xdict['atts']
    vars(opts).update(atts)
    assert opts.site in SITES, "site not supported in DB %s"%opts.site
    taskName = unicode(opts.tname)
    t_type = unicode(opts.t_type)
    n_instances = int(opts.Ninstances)
    site = unicode(opts.site)
    res = requests.post("%s/job/" % DAMPE_WORKFLOW_URL,
                        data={"taskname": taskName, "t_type": t_type, "n_instances": n_instances, "site" : site},
                        files={"file":open(xmlFile, "rb")})
    res.raise_for_status()
    if res.json().get("result", "nok") == "ok":
        print 'Added job %s with %i instances'%(taskName,n_instances)
    else:
        print "Error message: %s" % res.json().get("error", "")

if __name__ == "__main__":
    main()
