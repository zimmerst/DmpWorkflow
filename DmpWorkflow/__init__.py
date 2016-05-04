from pkgutil import extend_path
import logging
__path__ = extend_path(__path__, __name__)

# Define Version

majorVersion = 0
minorVersion = 1
patchLevel   = 0
preVersion   = 1
    
version      = "v%sr%s" % ( majorVersion, minorVersion )
buildVersion = "v%dr%d" % ( majorVersion, minorVersion )
if patchLevel:
    version = "%sp%s" % ( version, patchLevel )
    buildVersion = "%s build %s" % ( buildVersion, patchLevel )
if preVersion:
    version = "%s-pre%s" % ( version, preVersion )
    buildVersion = "%s pre %s" % ( buildVersion, preVersion )

from utils.logger import initLogger
from config.defaults import cfg
try:
    logfile = cfg.get("global","logfile")
    initLogger(logfile)
except Exception:
    logging.warning("Log service client was not initialized properly")
except ImportError:
    pass
