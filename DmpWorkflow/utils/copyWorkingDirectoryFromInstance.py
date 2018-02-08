#!/usr/bin/env python
## convenience script to extract JobDirectory and copy to local space
# call: python script.py -t JobName -i instanceId -o OutputDir
#
from copy import deepcopy
from os import curdir, mkdir, system, getcwd
from os.path import abspath,isdir
from sys import argv
from argparse import ArgumentParser

def getSixDigits(number, asPath=False):
  """ since we can have many many streams, break things up into chunks, 
      this should make sure that 'ls' is not too slow. """
  if not asPath:
     return str(number).zfill(6)
  else:
     if number < 100:
        return str(number).zfill(2)
     else:
          my_path = []
          rest = deepcopy(number)
          blocks = [100000, 10000, 1000, 100]
          for b in blocks:
              value, rest = divmod(rest, b)
              # print b, value, rest
              if value:
                padding = "".ljust(len(str(b)) - 1, "x")
                my_path.append("%i%s" % (value, padding))
                rest = rest
          my_path.append(str(rest).zfill(2))
          return "/".join([str(s) for s in my_path])

def main(args=None):
  usage = "Usage: %(prog)s [options]"
  description = "extract Working Directory based on JobName & Instance Id"
  parser = ArgumentParser(usage=usage, description=description)
  parser.add_argument("--job","-j", dest='task', help="name of job",required=True)
  parser.add_argument("--type","-t", dest="task_type", help="type of task, Simulation is default", default="Simulation")
  parser.add_argument("--site","-s", dest='site', default="BARI", help="Name of site the job was running at")
  parser.add_argument("--inst_id","-i", dest='inst_id', help="Instance ID", type=int,required=True)
  parser.add_argument("--workdir_path", dest='wdpth', default="mc/workdir",help="Path of working directory in remote location")
  parser.add_argument("--gridftp_path", dest='rmtpth', default='stzimmer@gridftp.cscs.ch/scratch/snx3000/stzimmer',help="Full path for gridFTP transfer")
  #parser.add_argument("--output","-o",dest='output',help='output directory, if not set, will create directory here',default=None)
  opts = parser.parse_args(args)
  #outdir=abspath(curdir)
  #if not opts.output is None:
  #  outdir=opts.output
  #if not isdir(outdir): mkdir(outdir) 
  if site == 'CSCS':
	xrd_cmd = "globus-url-copy -r -sync -q -rst sshftp://{remotepath}/{wdpath}/{site}/{task}/{task_type}/{sixDigit} file://{cwd}/{task}/{inst}/. 2> /dev/null".format(site=opts.site, task=opts.task, 
																																									sixDigit=getSixDigits(opts.inst_id,asPath=True),
																																									task_type=opts.task_type, cwd=getcwd(),
																																									wdpath=opts.wdpth, remotepath=opt.rmtpth)
	# r: recursive ; sync: no copy if file already exists; q: quiet mode; rst: restart if failed, up to 5 times
  
  else:
    xrd_files="xrdfs xrootd-dampe.cloud.ba.infn.it ls -u /UserSpace/dampe_prod/{wdpath}/{site}/{task}/{task_type}/{sixDigit} 2> /dev/null".format(site=opts.site, task=opts.task, sixDigit=getSixDigits(opts.inst_id,asPath=True),task_type=opts.task_type,wdpath=opts.wdpth)
    xrd_cmd="{list} | xargs -I @ xrdcp @ {task}/{inst}/.".format(list=xrd_files,inst=opts.inst_id,task=opts.task)
  
  mkcmd="mkdir -pv {task}/{inst}".format(task=opts.task,inst=opts.inst_id)
  print mkcmd
  print xrd_cmd





  pass

if __name__ == '__main__':
  main()
