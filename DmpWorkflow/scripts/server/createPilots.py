'''
Created on Dec 8, 2016

@author: zimmer
@brief: create pilot instances based on the number of instances that are needed per site...
'''
from yaml import load as yload
from os import getpid
from os.path import isfile, join as pjoin
from datetime import datetime
from tqdm import tqdm
from json import loads
from time import sleep
from copy import deepcopy
from argparse import ArgumentParser
from DmpWorkflow.config.defaults import DAMPE_WORKFLOW_ROOT
from DmpWorkflow.core.models import JobInstance, Job

cfg_default_path=pjoin(DAMPE_WORKFLOW_ROOT,"/config/pilot.yaml")

def yaml_load(fi):
    return yload(open(fi,'rb').read())

def write_pidfile(fi):
    fo = open(fi,"w")
    fo.write(getpid())
    fo.close()

def read_pidfile(fi):
    fo = open(fi,"r")
    fout = fo.read()
    fo.close()
    return fout

def main(args=None):
    parser = ArgumentParser(usage="Usage: %(prog)s [options]", description="create pilot instances based on pilot jobs")
    parser.add_argument("-c","--config",dest="cfg",default=cfg_default_path, type=str, help="location of configuration file for pilots (in yaml)")
    parser.add_argument('-d','--dry',action='store_true',dest='dry',help='do not reap but show what you would reap (dry-run)')
    opts = parser.parse_args(args)
    
    update_dict  =  {
                        "created_at"    : datetime.now(),
                        "last_update"   : datetime.now(),
                        "batchId"       : None,
                        "Nevents"       : 0,
                        "hostname"      : None,
                        "status"        : "New",
                        "minor_status"  : "AwaitingBatchSubmission",
                        "status_history": [],
                        "memory"        : [],
                        "cpu"           : [],
                        "pilotReference": None,
                        "log"           : ""
                    }
    
    
    if not isfile(opts.cfg):
        raise IOError("could not find configuration file: %s",opts.cfg)
    
    ## in any case, first read defaults
    cfg = yaml_load(cfg_default_path)    
    if opts.cfg != cfg_default_path:
        cfg.update(yaml_load(opts.cfg))

    if isfile(cfg['global']['pidfile']):
        raise Exception("found running pilot agent, check process %s",read_pidfile(cfg['global']['pidfile']))
    
    write_pidfile(cfg['global']['pidfile'])
    default_pilot = cfg['default-pilot']
    while isfile(cfg['global']['pidfile']) and read_pidfile(cfg['global']['pidfile']) == str(getpid()):
        ## run loop
        for pilot in tqdm(cfg['pilots']):
            my_pilot = deepcopy(default_pilot)
            my_pilot.update(pilot)
            if not JobInstance.objects.filter(status="New", site=my_pilot['site'], isPilot=False): continue
            # find mother pilot
            pilot = None
            try:
                if my_pilot['version'] == 'None':   
                    print 'no version specified, use latest of type pilot'
                    query = Job.objects.filter(type='Pilot',site=my_pilot['site'])
                    if query.count(): pilot = query.first()
                else:
                    pilot = Job.objects.get(type="Pilot",site=my_pilot['site'],release=my_pilot['version'])
            except Job.DoesNotExist:
                raise Exception("could not find pilot corresponding to site & version provided in configuration")
            if pilot is None:
                raise Exception("could not find pilot")
            print 'query running pilots'
            query = JobInstance.objects.filter(isPilot=True, job=pilot)
            running_pilots = query.filter(status__in=["New","Submitted","Running"]).count()
            pilots_to_fill = int(my_pilot['pilotsPerSite']) - running_pilots
            if pilots_to_fill:
                blueprint_js = loads(query.first().to_json())
                for key in [u'instanceId', u'created_at', u'_cls', u'_id']:
                    if key in blueprint_js: blueprint_js.pop(key)
                blueprint_js = blueprint_js.update(update_dict)
                pilotInstances = [JobInstance(**blueprint_js)] * pilots_to_fill
                for p in pilotInstances:
                    p.setAsPilot(True)
                    pilot.addInstance(p)
                pilot.save()
        ## done loop
        print 'sleeping for pre-determined time {sleep}...'.format(sleep=cfg['global']['sleeptime'])
        sleep(cfg['global']['sleeptime'])

if __name__ == "__main__":
    main()