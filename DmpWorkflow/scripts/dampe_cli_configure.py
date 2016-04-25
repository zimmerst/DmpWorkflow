'''
Created on Apr 25, 2016

@author: zimmer
@brief: convenience script to modify configuration parameters from an existing file.
@todo: add verification of configuration file, pre-parsing etc.
'''
import os
import DmpWorkflow
from DmpWorkflow.utils.tools import safe_copy
from argparse import ArgumentParser

def main(args=None):
    parser = ArgumentParser(usage="Usage: %prog [options]", description="initialize dampe workflow")
    parser.add_argument("-f","--file",dest="file",type=str,default=None, help='use this flag if you plan to provide a file')
    opts = parser.parse_args(args)
    if opts.file is not None:
        src = opts.file
        dmpROOT = os.path.dirname(os.path.abspath(DmpWorkflow.__file__))
        tg  = os.path.join(dmpROOT,"config/settings.cfg")
        safe_copy(src, tg, sleep='3s', attempts = 3, debug=True)
        return 
    
if __name__ == "__main__":
    main()

