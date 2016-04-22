"""
Created on Apr 20, 2016

@author: zimmer
@brief: prototype script that handles config parsing etc.

"""
import ConfigParser, os, sys
import DmpWorkflow
from DmpWorkflow.utils.tools import exceptionHandler

myDefaults = {
    "DAMPE_SW_DIR": ".",
    "ExternalsScript": "${DAMPE_SW_DIR}/setup/setup.sh",
    "use_debugger": "true",
    "use_reloader": "true",
    "traceback": "true",
    "task_types": "Generation,Digitization,Reconstruction,User,Other".split(","),
    "task_major_statii": "New,Running,Failed,Terminated,Done,Submitted,Suspended".split(",")
}

DAMPE_WORKFLOW_ROOT = os.path.dirname(DmpWorkflow.__file__)

cfg = ConfigParser.SafeConfigParser(defaults=myDefaults)
cfg.read(os.path.join(DAMPE_WORKFLOW_ROOT,"config/settings.cfg"))

os.environ["DAMPE_SW_DIR"] = cfg.get("site", "DAMPE_SW_DIR")
os.environ["DAMPE_WORKFLOW_ROOT"] = DAMPE_WORKFLOW_ROOT

# print "setting up externals"
#source_bash(cfg.get("site", "ExternalsScript"))

dbg = cfg.getboolean("global", "traceback")
if not dbg:
    sys.excepthook = exceptionHandler
