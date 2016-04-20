'''
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
'''
from argparse import ArgumentParser

from utils.scriptDefaults import cfg
from core.models import Job, JobInstance, TYPES
from utils.flask_helpers import parseJobXmlToDict
from core import db
import random, sys, os
from werkzeug.exceptions import NotFound

_TYPES = list(TYPES) + ["NONE"]

dummy_dict = {"InputFiles": [], "OutputFiles": [], "MetaData": []}


def main(args=None):
    description = "create new instances for job in DB"
    parser = ArgumentParser(usage="Usage: %prog taskName xmlFile [options]", description=description)
    parser.add_argument("--instance", dest="inst", type=int, default=None,
                        help='use this to offset an instance')
    opts = parser.parse_args()
    # if len(sys.argv)!=3:
    #    print parser.print_help()
    #    raise Exception
    taskName = sys.argv[1]
    ninst = int(sys.argv[2])
    db.connect()
    jobs = Job.objects.filter(title=taskName)
    if len(jobs):
        job = jobs[0]
        os.environ['DWF_JOBNAME'] = job.title
        dout = parseJobXmlToDict(job.body)
        if 'type' in dout['atts']:
            job.type = dout['atts']['type']
        if 'release' in dout['atts']:
            job.release = dout['atts']['release']
        if ninst:
            for j in range(ninst):
                jI = JobInstance(body=str(dummy_dict))
                if opts.inst and j == 0:
                    job.addInstance(jI, inst=opts.inst)
                else:
                    job.addInstance(jI)
        # print len(job.jobInstances)
        job.update()
        print 'added %i new instances for job %s' % (ninst, taskName)
    else:
        raise NotFound('could not find job %s' % taskName)


if __name__ == '__main__':
    main()
