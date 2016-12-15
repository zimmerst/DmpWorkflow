'''
Created on Oct 4, 2016

@author: zimmer
@brief: reaper, kills instances whose last update has been too long ago.
'''
from argparse import ArgumentParser
from DmpWorkflow.core.models import JobInstance
from DmpWorkflow.utils.tools import send_heartbeat
from DmpWorkflow.config.defaults import DAMPE_VERSION
from datetime import datetime, timedelta

def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s [options]", description="reap instances which have not send any heartbeat")
    parser.add_argument('-t','--time',type=int,default=360,dest='time',help='timedelta in minutes to be used by reaper to kill jobs')
    parser.add_argument('-d','--dry',action='store_true',dest='dry',help='do not reap but show what you would reap (dry-run)')
    opts = parser.parse_args(args)
    send_heartbeat("JobReaper", DAMPE_VERSION)
    past = datetime.now() - timedelta(minutes=opts.time)
    query = JobInstance.objects.filter(status="Running",last_update__lte=past)
    clen = query.count()
    if not clen:
        print 'no instances found to reap, return'
        return
    print 'found %i instances to potentially reap'%clen
    if not opts.dry:
        res=query.update(status="Terminated",minor_status="KilledByReaper")
        print 'reaped %i instances'%res

if __name__ == "__main__":
    main()