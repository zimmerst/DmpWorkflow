'''
Created on Mar 30, 2016

@author: zimmer
@brief: prototype script to create a new job from the jobXml
'''
from core.models import Job, JobInstance, TYPES
from core import db
import random, sys

dummy_dict = {"InputFiles":[],"OutputFiles":[],"MetaData":[]}

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    usage = "Usage: %prog taskName xmlFile [options]"
    description = "update job in DB"
    parser.set_usage(usage)
    parser.set_description(description)
    parser.add_option("--type", dest="type",type='choice', default = "User",help='minor status',choices=TYPES)
    parser.add_option("--Ninstances", dest="Ninstances",type=int, default = 0,
                      help='number of instances to create at the same time')
    (opts, arguments) = parser.parse_args()
    #if len(sys.argv)!=3:
    #    print parser.print_help()
    #    raise Exception
    taskName        = sys.argv[1]
    xmlFile         = sys.argv[2]
    db.connect()
    job = Job(title=taskName, body=open(xmlFile,'r').read(), type=opts.type)
    if opts.Ninstances:
        for j in range(random.randrange(opts.Ninstances)):
            jI = JobInstance(body=str(dummy_dict))
            job.addInstance(jI)
    job.save()
    print 'created job %s with %i new instances'%(taskName,opts.Ninstances)
    