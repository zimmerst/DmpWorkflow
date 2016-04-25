"""
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
"""
import requests
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_URL
from DmpWorkflow.core.models import TYPES, log
# from DmpWorkflow.utils.db_helpers import parseJobXmlToDict

_TYPES = list(TYPES) + ["NONE"]

# dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}


def main(args=None):
    parser = ArgumentParser(usage="Usage: %prog taskName xmlFile [options]", description="create new job in DB")
    parser.add_argument("-t", "--type", dest="t_type", type=str, default="", help='task type', choices=_TYPES)
    parser.add_argument("--Ninstances", dest="Ninstances", type=int, default=0,
                        help='number of instances to create at the same time')
    parser.add_argument("-i", "--input", dest="xml", help="Path to job XML")
    parser.add_argument("-n", '--name', help="task Name", dest="tname")
    opts = parser.parse_args(args)s
    # if len(sys.argv)!=3:
    #    print parser.print_help()
    #    raise Exception
    taskName = opts.tname
    xmlFile = opts.xml
    t_type = opts.t_type
    n_instances = opts.Ninstances
    res = requests.post("%s/job/" % DAMPE_WORKFLOW_URL,
                        data={"taskname": taskName, "t_type": t_type, "n_instances": n_instances},
                        files={"file":open(xmlFile, "rb")})
    try:
        res.raise_for_status()
    except Exception as err:
        log.exception(err)
    if res.json().get("result", "nok") == "ok":
        print 'Added job'
    else:
        print "Error message: %s" % res.json().get("error", "")
    # db.connect()
    # job = Job(title=taskName, body=open(xmlFile, 'r').read())
    # os.environ['DWF_JOBNAME'] = job.title
    # dout = parseJobXmlToDict(job.body)
    # if 'type' in dout['atts']:
    #     job.type = dout['atts']['type']
    # if 'release' in dout['atts']:
    #     job.release = dout['atts']['release']
    # if not opts.type == "NONE":
    #     job.type = opts.type
    # if opts.Ninstances:
    #     for j in range(opts.Ninstances):
    #         jI = JobInstance(body=str(dummy_dict))
    #         job.addInstance(jI)
    # # print len(job.jobInstances)
    # job.save()
    # print 'created job %s with %i new instances' % (taskName, opts.Ninstances)

if __name__ == "__main__":
    main()
