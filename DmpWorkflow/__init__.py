import logging

# Define Version
import pkg_resources
version = pkg_resources.get_distribution(__package__).version

try:
    from DmpWorkflow.config.defaults import DAMPE_LOGFILE
    from DmpWorkflow.utils.logger import initLogger

    initLogger(DAMPE_LOGFILE)
    from DmpWorkflow.utils.logger import logger_batch, logger_script, logger_core
except Exception as err:
    logging.warning("Log service client was not initialized properly: %s", str(err))
