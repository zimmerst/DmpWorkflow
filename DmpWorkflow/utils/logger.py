'''
Created on May 4, 2016

@author: zimmer
'''

from logging.config import dictConfig
import logging

def initLogger(logfile):
    # add logger
    LOGGING = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                            "precise": {
                                        "format": "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
                                        'datefmt': '%Y-%m-%d %H:%M:%S'
                                        }                           
                           },
            'handlers': {
                            'console':     {
                                            'level': 'DEBUG',
                                            'class': 'logging.StreamHandler',
                                            },
                            'file':         {
                                             'level': "INFO",
                                             'formatter': "precise",
                                             'class': 'logging.handlers.RotatingFileHandler',
                                             'filename': logfile,
                                             'maxBytes': "2000000",
                                             'backupCount': 5
                                             }
                        },
            'loggers': {
                        'app': {
                                'handlers': ['console', "file"],
                                'level': 'INFO'
                                },
                        'script':{
                                  'handlers': ['console'],
                                  'level':'INFO'
                                }
                                
                        
                        }
                   }
    dictConfig(LOGGING)