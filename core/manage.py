'''
Created on Mar 1, 2016

@author: zimmer
'''
import os, time, sys, random
# Set the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime
from flask.ext.script import Manager, Server
from core import app, cfg

seed = cfg.get("global","randomSeed")
seed = int(seed) if not seed=='true' else int(time.mktime(datetime.now().timetuple()))
#random.seed(seed)

manager = Manager(app)
# Turn on debugger by default and reloader
manager.add_command(
        "runserver",
                Server(
                       use_debugger = cfg.getboolean("server","use_debugger"),
                       use_reloader = cfg.getboolean("server","use_reloader"),
                       host = cfg.get("server","host")
                      )
                    )

if __name__ == "__main__":
    #print("initializing random seed %i"%seed)
    manager.run()