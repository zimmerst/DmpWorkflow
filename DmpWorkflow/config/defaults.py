"""
Created on Apr 20, 2016

@author: zimmer
@brief: prototype script that handles config parsing etc.

"""
import ConfigParser
import os
import sys

import DmpWorkflow
from DmpWorkflow.utils.tools import exceptionHandler

DAMPE_WORKFLOW_ROOT = os.path.dirname(DmpWorkflow.__file__)

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
    "HPCextras" : "",
    "HPCqueue" : "",
    "HPCcputime" : "24:00",
    "HPCmemory": "1000."
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

os.environ["BATCH_SYSTEM"] = cfg.get("site","HPCsystem")
os.environ["BATCH_REQUIREMENTS"] = cfg.get("site","HPCrequirements")
os.environ["BATCH_EXTRAS"] = cfg.get("site","HPCextras")
os.environ["BATCH_QUEUE"] = cfg.get("site","HPCqueue")

BATCH_DEFAULTS = {key:os.getenv("BATCH_%s"%key.upper()) for key in ['system','requirements','extras','queue']}
BATCH_DEFAULTS['memory']=cfg.get("site","HPCmemory")
BATCH_DEFAULTS['cputime']=cfg.get("site","HPCcputime")
BATCH_DEFAULTS['name']=cfg.get("site","name")
# verify that the site configuration is okay.
assert BATCH_DEFAULTS['name'] in cfg.get("JobDB","batch_sites"), "Batch site %s not in DB"%BATCH_DEFAULTS['name']
assert BATCH_DEFAULTS['system'] in ["lsf","sge"], "HPCSystem %s not supported."%BATCH_DEFAULTS["system"]
