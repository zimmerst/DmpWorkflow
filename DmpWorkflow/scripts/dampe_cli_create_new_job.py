"""
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
"""
import requests
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL, TYPES, SITES
# from DmpWorkflow.utils.db_helpers import parseJobXmlToDict

_TYPES = list(TYPES) + [u"NONE"]

# dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}


def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s taskName xmlFile [options]", description="create new job in DB")
    parser.add_argument("-t", "--type", dest="t_type", type=str, default="", help='task type', choices=_TYPES)
    parser.add_argument("--Ninstances", dest="Ninstances", type=int, default=0,
                        help='number of instances to create at the same time')
    parser.add_argument("-i", "--input", dest="xml", help="Path to job XML")
    parser.add_argument("-n", '--name', help="task Name", dest="tname")
    parser.add_argument("-s", '--site', help="site to run at", dest="site")
    opts = parser.parse_args(args)

    taskName = unicode(opts.tname)
    xmlFile = unicode(opts.xml)
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
