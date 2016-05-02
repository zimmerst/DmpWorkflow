"""
Created on Apr 20, 2016

@author: zimmer
@brief: prototype script that handles config parsing etc.

"""
import ConfigParser
import os
import sys
import logging
from DmpWorkflow import __path__ as DMPPath
from DmpWorkflow.utils.tools import exceptionHandler

def AppLogger(name,level=logging.INFO):
    log = logging.getLogger(name)
    log.setLevel(level)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)
    return log

DAMPE_WORKFLOW_ROOT = os.path.abspath(DMPPath[0])

__myDefaults = {
    "DAMPE_SW_DIR": ".",
    "installation": "server",
    "ExternalsScript": "${DAMPE_SW_DIR}/setup/setup.sh",
    "use_debugger": "true",
    "use_reloader": "true",
    "workdir" : ".",
    "url" : "",
    "traceback": "true",
    "task_types": "Generation,Digitization,Reconstruction,User,Other".split(","),
    "task_major_statii": "New,Running,Failed,Terminated,Done,Submitted,Suspended".split(","),
    "HPCsystem" : "lsf",
    "HPCrequirements" : "",
    "HPCextra" : "",
    "HPCqueue" : "",
    "HPCcputime" : "24:00",
    "HPCmemory": "1000.",
    "EXEC_DIR_ROOT" : "/tmp"
    "ratio_mem" : "1.0",
    "ratio_cpu" : "1.0",
}

cfg = ConfigParser.SafeConfigParser(defaults=__myDefaults)

cfg.read(os.path.join(DAMPE_WORKFLOW_ROOT, "config/settings.cfg"))

assert cfg.get("global","installation") in ['server','client'], "installation must be server or client!"

os.environ["DAMPE_SW_DIR"] = cfg.get("site", "DAMPE_SW_DIR")
os.environ["DAMPE_WORKFLOW_ROOT"] = DAMPE_WORKFLOW_ROOT

#os.environ["DAMPE_URL"] = cfg.get("server","url")
# print "setting up externals"
#source_bash(cfg.get("site", "ExternalsScript"))

dbg = cfg.getboolean("global", "traceback")
if not dbg:
    sys.excepthook = exceptionHandler

DAMPE_WORKFLOW_URL = cfg.get("server", "url")
DAMPE_WORKFLOW_DIR = cfg.get("site","workdir")
EXEC_DIR_ROOT = cfg.get("site","EXEC_DIR_ROOT")

os.environ["BATCH_SYSTEM"] = cfg.get("site","HPCsystem")
os.environ["BATCH_REQUIREMENTS"] = cfg.get("site","HPCrequirements")
os.environ["BATCH_EXTRA"] = cfg.get("site","HPCextra")
os.environ["BATCH_QUEUE"] = cfg.get("site","HPCqueue")
os.environ["EXEC_DIR_ROOT"] = EXEC_DIR_ROOT

BATCH_DEFAULTS = {key:os.getenv("BATCH_%s"%key.upper()) for key in ['system','requirements','extra','queue']}
BATCH_DEFAULTS['memory']=cfg.get("site","HPCmemory")
BATCH_DEFAULTS['cputime']=cfg.get("site","HPCcputime")
BATCH_DEFAULTS['name']=cfg.get("site","name")

# JobDB specifics
MAJOR_STATII = tuple([unicode(t) for t in cfg.get("JobDB", "task_major_statii").split(",")])
FINAL_STATII = tuple([unicode(t) for t in cfg.get("JobDB", "task_final_statii").split(",")])
TYPES = tuple([unicode(t) for t in cfg.get("JobDB", "task_types").split(",")])
SITES = tuple([unicode(t) for t in cfg.get("JobDB", "batch_sites").split(",")])

# verify that the site configuration is okay.
assert BATCH_DEFAULTS['name'] in cfg.get("JobDB","batch_sites"), "Batch site %s not in DB"%BATCH_DEFAULTS['name']
assert BATCH_DEFAULTS['system'] in ["lsf","sge"], "HPCSystem %s not supported."%BATCH_DEFAULTS["system"]