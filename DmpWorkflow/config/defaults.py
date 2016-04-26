"""
Created on Apr 20, 2016

@author: zimmer
@brief: prototype script that handles config parsing etc.

"""
import ConfigParser
import os
import sys
from DmpWorkflow.utils.tools import exceptionHandler

def getPath():
    import DmpWorkflow as DWF
    return DWF.__path__

__myDefaults = {
    "DAMPE_SW_DIR": ".",
    "ExternalsScript": "${DAMPE_SW_DIR}/setup/setup.sh",
    "use_debugger": "true",
    "use_reloader": "true",
    "traceback": "true",
    "task_types": "Generation,Digitization,Reconstruction,User,Other".split(","),
    "task_major_statii": "New,Running,Failed,Terminated,Done,Submitted,Suspended".split(",")
}

DAMPE_WORKFLOW_URL = getPath()

cfg = ConfigParser.SafeConfigParser(defaults=__myDefaults)

cfg.read(os.path.join(DAMPE_WORKFLOW_ROOT, "config/settings.cfg"))

os.environ["DAMPE_SW_DIR"] = cfg.get("site", "DAMPE_SW_DIR")
os.environ["DAMPE_WORKFLOW_ROOT"] = DAMPE_WORKFLOW_ROOT

#os.environ["DAMPE_URL"] = cfg.get("server","url")
# print "setting up externals"
#source_bash(cfg.get("site", "ExternalsScript"))

dbg = cfg.getboolean("global", "traceback")
if not dbg:
    sys.excepthook = exceptionHandler

DAMPE_WORKFLOW_URL = cfg.get("server", "url")
