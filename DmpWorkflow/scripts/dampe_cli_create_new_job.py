"""
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
"""
import os
import sys
from DmpWorkflow.config.defaults import cfg, ArgumentParser
from DmpWorkflow.core import db
from DmpWorkflow.core.models import Job, JobInstance, TYPES
from DmpWorkflow.utils.flask_helpers import parseJobXmlToDict

_TYPES = list(TYPES) + ["NONE"]

dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}


def main(args=None):
    parser = ArgumentParser(usage="Usage: %prog taskName xmlFile [options]", description="update job in DB")
    parser.add_argument("--type", dest="type", type='choice', default="NONE", help='minor status', choices=_TYPES)
    parser.add_argument("--Ninstances", dest="Ninstances", type=int, default=0,
                        help='number of instances to create at the same time')
    opts = parser.parse_args(args)
    # if len(sys.argv)!=3:
    #    print parser.print_help()
    #    raise Exception
    taskName = sys.argv[1]
    xmlFile = sys.argv[2]
    db.connect()
    job = Job(title=taskName, body=open(xmlFile, 'r').read())
    os.environ['DWF_JOBNAME'] = job.title
    dout = parseJobXmlToDict(job.body)
    if 'type' in dout['atts']:
        job.type = dout['atts']['type']
    if 'release' in dout['atts']:
        job.release = dout['atts']['release']
    if not opts.type == "NONE":
        job.type = opts.type
    if opts.Ninstances:
        for j in range(opts.Ninstances):
            jI = JobInstance(body=str(dummy_dict))
            job.addInstance(jI)
    # print len(job.jobInstances)
    job.save()
    print 'created job %s with %i new instances' % (taskName, opts.Ninstances)

if __name__ == "__main__":
    main()
