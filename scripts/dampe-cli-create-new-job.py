'''
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
'''
from core.models import Job, JobInstance, TYPES
from utils.flask_helpers import parseJobXmlToDict
from core import db
import random, sys

_TYPES = list(TYPES)+["NONE"]

dummy_dict = {"InputFiles":[],"OutputFiles":[],"MetaData":[]}

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    usage = "Usage: %prog taskName xmlFile [options]"
    description = "update job in DB"
    parser.set_usage(usage)
    parser.set_description(description)
    parser.add_option("--type", dest="type",type='choice', default = "NONE",help='minor status',choices=_TYPES)
    parser.add_option("--Ninstances", dest="Ninstances",type=int, default = 0,
                      help='number of instances to create at the same time')
    (opts, arguments) = parser.parse_args()
    #if len(sys.argv)!=3:
    #    print parser.print_help()
    #    raise Exception
    taskName        = sys.argv[1]
    xmlFile         = sys.argv[2]
    db.connect()
    ## collision-check for dummies!
    req = Job.objects.filter(title=taskName)
    if req: raise Exception("a task with the specified name exists already.")
    
    job = Job(title=taskName, body=open(xmlFile,'r').read())
    dout = parseJobXmlToDict(job.body)
    if 'type' in dout['atts']: job.type = dout['atts']['type']
    if 'release' in dout['atts']: job.release = dout['atts']['release']
    if not opts.type == "NONE": job.type = opts.type
    if opts.Ninstances:
        for j in range(opts.Ninstances):
            jI = JobInstance(body=str(dummy_dict))
            job.addInstance(jI)
    print len(job.jobInstances)
    job.save()
    print 'created job %s with %i new instances'%(taskName,opts.Ninstances)
    