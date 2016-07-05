"""
Created on May 4, 2016

@author: zimmer
"""

import logging
import logging.config
from logging import Formatter, StreamHandler
from logging.handlers import RotatingFileHandler
from DmpWorkflow.config.defaults import DAMPE_LOGFILE

log_path = DAMPE_LOGFILE

logger_core = logging.getLogger("core")
handler_file = RotatingFileHandler(maxBytes=2000000, filename=log_path, backupCount=5)
form = Formatter("[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s", '%Y-%m-%d %H:%M:%S')
handler_file.setFormatter(form)
logger_core.addHandler(handler_file)
logger_core.setLevel(logging.INFO)

logger_script = logging.getLogger("script")
handler_console = StreamHandler()
handler_console.setFormatter(form)
logger_script.addHandler(handler_console)
logger_script.setLevel(logging.DEBUG)

logger_batch = logging.getLogger("batch")
logger_batch.addHandler(handler_console)
logger_batch.setLevel(logging.INFO)


def initLogger(logfile):
    # add logger
    cfg = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            "precise": {
                "format": "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
            },
            'file': {
                'level': "INFO",
                'formatter': "precise",
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': logfile,
                'maxBytes': "2000000",
                'backupCount': 5
            }
        },
        'loggers': {
            'root': {
                'handlers': ["file"],
                'level': 'INFO'
            },
            'core': {
                'handlers': ["file"],
                'level': 'INFO'
            },
            'script': {
                'handlers': ['console'],
                'level': 'DEBUG'
            },
            'batch': {
                'handlers': ['console'],
                'level': 'INFO'
            }

        }
    }
    logging.config.dictConfig(cfg)
