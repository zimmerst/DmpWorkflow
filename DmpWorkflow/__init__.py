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

from DmpWorkflow.utils.logger import initLogger
import logging
try:
    from DmpWorkflow.config.defaults import DAMPE_LOGFILE
    initLogger(DAMPE_LOGFILE)
except Exception as err:
    logging.warning("Log service client was not initialized properly: %s" % str(err))
except ImportError:
    pass