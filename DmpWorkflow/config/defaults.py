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
    "ExternalsScript": "${DAMPE_SW_DIR}/setup/setup.sh",
    "use_debugger": "true",
    "use_reloader": "true",
    "traceback": "true",
    "task_types": "Generation,Digitization,Reconstruction,User,Other".split(","),
    "task_major_statii": "New,Running,Failed,Terminated,Done,Submitted,Suspended".split(","),
    "HPCsystem" : "lsf",
    "HPCrequirements" : "",
    "HPCextras" : "",
    "HPCqueuue" : ""
}

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

os.environ["BATCH_SYSTEM"] = cfg.get("site","HPCsystem")
os.environ["BATCH_REQUIREMENTS"] = cfg.get("site","HPCrequirements")
os.environ["BATCH_EXTRAS"] = cfg.get("site","HPCextras")
os.environ["BATCH_QUEUE"] = cfg.get("site","HPCqueue")

BATCH_DEFAULTS = {key:os.getenv("BATCH_%s"%key.upper()) for key in ['system','requirements','extras','queue']}

# verify that the site configuration is okay.
assert cfg.get("site","name") in cfg.get("JobDB","batch_sites"), "Batch site not in DB"
assert BATCH_DEFAULTS['system'] in ["lsf","sge"], "HPCSystem not supported."
