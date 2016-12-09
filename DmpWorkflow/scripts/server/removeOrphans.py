'''
Created on Nov 24, 2016

@author: zimmer
@brief: remove orphaned instances
'''
from argparse import ArgumentParser
from DmpWorkflow.core.models import JobInstance, Job

def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s [options]", description="reap instances which have not send any heartbeat")
    parser.add_argument('-d','--dry',action='store_true',dest='dry',help='do not reap but show what you would reap (dry-run)')
    opts = parser.parse_args(args)
    query = JobInstance.objects.filter(job__nin=Job.objects.all())
    clen = query.count()
    if not clen:
        print 'no instances found to remove, return'
        return
    print 'found %i instances to potentially delete'%clen
    if not opts.dry:
        res = query.delete()
        print 'reaped %i instances'%res

if __name__ == "__main__":
    main()