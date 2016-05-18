'''
Created on May 18, 2016

@author: zimmer
@brief: server-side script to remove duplicate jobInstances.
'''
import progressbar
from DmpWorkflow.core.models import JobInstance

def main():
    try:
        from DmpWorkflow.core import db
        db.connect()
    except Exception as err:
        import sys
        print 'could not import DB, is this configuration a server setup?'
        print 'ERROR: %s'%err
        sys.exit(0)
    instances = JobInstance.objects.all()
    print 'found %i instances'%len(instances)
    pb = progressbar.ProgressBar(maxval=len(instances))
    
    duplicates = []
    good_instances = []
    pb.start()
    for i,inst in enumerate(instances):
        if inst.instanceId in good_instances:
            duplicates.append(inst)
        else:
            good_instances.append(inst.instanceId)
        pb.update(i+1)
    pb.finish()
    print 'found %i duplicates'%len(duplicates)
    if len(duplicates):
        print 'removing...'
        pb = progressbar.ProgressBar(maxval=len(duplicates))
        pb.start()
    for i, inst in enumerate(duplicates):
        # do cleanup
        ref_instance = inst.job.getInstance(inst.instanceId)
        if ref_instance.id != inst.id:
            #print inst.instanceId
            inst.delete()
        pb.update(i+1)
    pb.finish()
    print 'done'

if __name__ == "__main__":
    main()
