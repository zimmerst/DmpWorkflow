"""
Created on Mar 25, 2016

@author: zimmer
"""
import logging
import shutil
from os import makedirs, environ, utime, system
from os.path import exists, expandvars
from sys import stdout
from random import choice, randint
from shlex import split as shlex_split
from string import ascii_letters, digits
import subprocess as sub
from time import sleep as time_sleep
from re import split as re_split
from datetime import timedelta
from copy import deepcopy
from StringIO import StringIO
from xml.dom import minidom as xdom
from hashlib import md5


def sortTimeStampList(my_list, timestamp='time', reverse=False):
    if not len(my_list):
        return []
    my_list = list(deepcopy(my_list))
    keys = sorted([v[timestamp] for v in my_list])
    if reverse: keys = reversed(keys)
    new_list = []
    for ts in keys:
        my_item = {timestamp: ts}
        # find the matching stamp in the original list
        for item in my_list:
            # print item #<-- debug
            if item[timestamp] == ts:
                del item[timestamp]
                my_item.update(item)
                new_list.append(my_item)
                my_list.remove(item)
    return new_list


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


def query_yes_no(question):
    print question + " [yes/no]"
    yes = {'yes', 'y', 'ye', ''}
    no = {'no', 'n'}
    choice = raw_input().lower()
    if choice in yes:
        ret = True
    elif choice in no:
        ret = False
    else:
        stdout.write("Please respond with 'yes' or 'no', aborting\n")
        ret = False
    return ret


def exceptionHandler(exception_type, exception, traceback):
    # All your trace are belong to us!
    # your format
    del traceback
    print "%s: %s" % (exception_type.__name__, exception)


def random_string_generator(size=16, chars=ascii_letters + digits):
    return ''.join(choice(chars) for _ in range(size))


def makeSafeName(srcname):
    rep = {".": "d", "+": "p", "-": "n"}
    for key in rep:
        srcname = srcname.replace(key, rep[key])
    return srcname


def pwd():
    # Careful, won't work after a call to os.chdir...
    return environ['PWD']


def mkdir(Dir):
    if not exists(Dir):
        makedirs(Dir)
    return Dir


def rm(pwd):
    try:
        system("rm -rf %s"%pwd)#shutil.rmtree(pwd)
    except Exception as err:
        logging.exception(err)


def mkscratch():
    if exists('/scratch/'):
        return mkdir('/scratch/%s/' % environ['USER'])
    elif exists('/tmp/'):
        return mkdir('/tmp/%s/' % environ['USER'])
    else:
        raise Exception('...')


def touch(path):
    with open(path, 'a'):
        utime(path, None)


def Ndigits(val, size=6):
    """ returns a N-digit integer with leading zeros """
    _sixDigit = "%i" % val
    return _sixDigit.zfill(size)


def safe_copy(infile, outfile, **kwargs):
    kwargs.setdefault('sleep', 10)
    kwargs.setdefault('attempts', 10)
    kwargs.setdefault('debug', False)
    kwargs.setdefault('checksum', False)
    kwargs.setdefault("checksum_blocksize", 4096)
    xrootd = False
    if kwargs['debug']:
        print 'cp %s -> %s' % (infile, outfile)
    infile = infile.replace("@", "") if infile.startswith("@") else infile
    # Try not to step on any toes....
    sleep = parse_sleep(kwargs['sleep'])
    if infile.startswith("root:"):
        print 'file is on xrootd - switching to XRD library'
        cmnd = "xrdcp %s %s" % (infile, outfile)
        xrootd = True
    else:
        infile = expandvars(infile)
        outfile = expandvars(outfile)
        cmnd = "cp %s %s" % (infile, outfile)
    md5in = md5out = None
    if kwargs['checksum'] and not xrootd:
        md5in = md5sum(infile, blocksize=kwargs['checksum_blocksize'])
    i = 1
    while i < kwargs['attempts']:
        if kwargs['debug'] and i > 0:
            print "Attempting to copy file..."
        status = sub.call(shlex_split(cmnd))
        if status == 0:
            if kwargs['checksum'] and not xrootd:
                md5out = md5sum(outfile, blocksize=kwargs['checksum_blocksize'])
            if md5in == md5out:
                return status
            else:
                print '%i - copy successful but checksum does not match, try again in 5s'
                time_sleep(5)
        else:
            print "%i - Copy failed; sleep %ss" % (i, sleep)
            time_sleep(sleep)
        i += 1
    raise IOError("Failed to copy file")


def parse_sleep(sleep):
    MINUTE = 60
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR
    WEEK = 7 * DAY
    if isinstance(sleep, float) or isinstance(sleep, int):
        return sleep
    elif isinstance(sleep, str):
        try:
            return float(sleep)
        except ValueError:
            pass

        if sleep.endswith('s'):
            return float(sleep.strip('s'))
        elif sleep.endswith('m'):
            return float(sleep.strip('m')) * MINUTE
        elif sleep.endswith('h'):
            return float(sleep.strip('h')) * HOUR
        elif sleep.endswith('d'):
            return float(sleep.strip('d')) * DAY
        elif sleep.endswith('w'):
            return float(sleep.strip('w')) * WEEK
        else:
            raise ValueError
    else:
        raise ValueError


def sleep(sleep):
    return time_sleep(parse_sleep(sleep))


class ResourceMonitor(object):
    memory = 0.
    usertime = 0.
    systime = 0.

    def __init__(self):
        self.query()

    def query(self):
        from resource import getrusage, RUSAGE_SELF
        usage = getrusage(RUSAGE_SELF)
        self.usertime = usage[0]
        self.systime = usage[1]
        # http://stackoverflow.com/questions/938733/total-memory-used-by-python-process
        self.memory = getrusage(RUSAGE_SELF).ru_maxrss * 1e-6  # mmemory in Mb

    def getMemory(self, unit='Mb'):
        self.query()
        if unit in ['Mb', 'mb', 'mB', 'MB']:
            return float(self.memory)
        elif unit in ['kb', 'KB', 'Kb', 'kB']:
            return float(self.memory) * 1024.
        elif unit in ['Gb', 'gb', 'GB', 'gB']:
            return float(self.memory) / 1024.
        return 0.

    def getCpuTime(self):
        self.query()
        return self.systime

    def getWallTime(self):
        self.query()
        return self.usertime

    def getEfficiency(self):
        self.query()
        return float(self.usertime) / float(self.systime)

    def __repr__(self):
        self.query()
        user = self.usertime
        sys = self.systime
        mem = self.memory
        return "usertime=%s systime=%s mem %s Mb" % (user, sys, mem)


def md5sum(filename, blocksize=65536):
    _hash = md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            _hash.update(block)
    dig = _hash.hexdigest()
    return dig


def camelize(myStr):
    d = "".join(x for x in str(myStr).title() if not x.isspace())
    return d


def random_with_N_digits(n):
    range_start = 10 ** (n - 1)
    range_end = (10 ** n) - 1
    return randint(range_start, range_end)


def convertHHMMtoSec(hhmm):
    vals = re_split(":", hhmm)
    if len(vals) == 2:
        h, m = vals[0], vals[1]
        s = 0
    elif len(vals) == 3:
        h, m, s = vals[0], vals[1], vals[2]
    else:
        raise Exception("not well formatted time string")
    return float(timedelta(hours=int(h), minutes=int(m), seconds=int(s)).total_seconds())


class JobXmlParser(object):
    def __init__(self, domInstance, parent="Job", setVars=True):
        self.setVars = setVars
        self.out = {}
        elems = xdom.parse(StringIO(domInstance)).getElementsByTagName(parent)
        if len(elems) > 1:
            print 'found multiple job instances in xml, will ignore everything but last.'
        if not len(elems):
            raise Exception('found no Job element in xml.')
        self.datt = dict(zip(elems[-1].attributes.keys(), [v.value for v in elems[-1].attributes.values()]))
        if setVars:
            for k, v in self.datt.iteritems():
                environ[k] = v
        self.nodes = [node for node in elems[-1].childNodes if isinstance(node, xdom.Element)]

    def __extractNodes__(self):
        """ private method, do not use """
        for node in self.nodes:
            name = str(node.localName)
            if name == "JobWrapper":
                self.out['executable'] = node.getAttribute("executable")
                self.out['script'] = node.firstChild.data
            else:
                if name in ["InputFiles", "OutputFiles"]:
                    my_key = "File"
                else:
                    my_key = "Var"
                section = []
                for elem in node.getElementsByTagName(my_key):
                    section.append(dict(zip(elem.attributes.keys(), [v.value for v in elem.attributes.values()])))
                self.out[str(name)] = section
                del section
        return self.out

    def __setVars__(self):
        """ private method, do not use """
        if self.setVars:
            for var in self.out['MetaData']:
                key = var['name']
                value = var['value']
                if "$" in value:
                    value = expandvars(value)
                environ[key] = value
                var['value'] = value
                # expand vars
        self.out['atts'] = self.datt
        if 'type' in self.datt:
            environ["DWF_TYPE"] = self.datt["type"]
        for var in self.out['InputFiles'] + self.out['OutputFiles']:
            if '$' in var['source']:
                var['source'] = expandvars(var['source'])
            if '$' in var['target']:
                var['target'] = expandvars(var['target'])
                # print var['source'],"->",var['target']
        return self.out

    def getResult(self):
        out = self.out
        out.update(self.__extractNodes__())
        out.update(self.__setVars__())
        return out


def parseJobXmlToDict(domInstance, parent="Job", setVars=True):
    xp = JobXmlParser(domInstance, parent=parent, setVars=setVars)
    out = xp.getResult()
    return out
