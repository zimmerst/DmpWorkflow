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
    usage = "Usage: %prog taskName xmlFile [options]"
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
    ninst = opts.inst
    res = requests.post("%s/jobInstances/" % DAMPE_WORKFLOW_URL,
                        data={"taskname": taskName, "n_instances": ninst})
    res.raise_for_status()
    res = res.json()
    if res.get("result", "nok") == "nok":
        print "Error : %s" % res.get("message", "")
    os.environ['DWF_JOBNAME'] = taskName

if __name__ == "__main__":
    main()

    # db.connect()
    # jobs = Job.objects.filter(title=taskName)
    # if len(jobs):
    #     job = jobs[0]
    #     os.environ['DWF_JOBNAME'] = job.title
    #     dout = parseJobXmlToDict(job.body)
    #     if 'type' in dout['atts']:
    #         job.type = dout['atts']['type']
    #     if 'release' in dout['atts']:
    #         job.release = dout['atts']['release']
    #     if ninst:
    #         for j in range(ninst):
    #             jI = JobInstance(body=str(dummy_dict))
    #             # if opts.inst and j == 0:
    #             #    job.addInstance(jI,inst=opts.inst)
    #             # else:
    #             job.addInstance(jI)
    #     # print len(job.jobInstances)
    #     job.update()
    #     print 'added %i new instances for job %s' % (ninst, taskName)
    # else:
    #     raise NotFound('could not find job %s' % taskName)
