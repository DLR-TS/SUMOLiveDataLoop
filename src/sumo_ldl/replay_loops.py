#!/usr/bin/env python
# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    replay_loops.py
# @author  Jakob Erdmann
# @author  Michael Behrisch
# @date    2013-09-02
"""
Run correction and simulation loop interleaved to emulate real-life behavior
only faster
"""
import os,sys
import signal
from subprocess import Popen, PIPE, STDOUT
import optparse

from . import tools

THIS_DIR = os.path.dirname(__file__)


def import_from_file(module_file):
    if module_file[-3:] == '.py':
        module_file = module_file[:-3]
    sys.path.append(os.path.dirname(module_file))
    return __import__(os.path.basename(module_file))

def get_options(args):
    optParser = optparse.OptionParser()
    optParser.add_option("-s", "--schema", help="the schema script to start")
    optParser.add_option("-r", "--region", help="REGION to use for both loops")
    optParser.add_option("-c", "--confFile", help="config file for loops")
    optParser.add_option("-l", "--log", help="write log to FILE", metavar="FILE")
    (options, args) = optParser.parse_args(args=args)
    return options

def read_until(process, keyword):
    line = "dummy"
    if process is None:
        return None
    while process.poll() is None and line is not None:
        line = process.stdout.readline()
        sys.stdout.write(line)
        sys.stdout.flush()
        if keyword in line:
            return line
    return None

def stop(process):
    if process is not None:
        try:
            os.kill(process.pid, signal.SIGSTOP)
        except OSError:
            print("could not stop process %s. already finished" % process.pid)

def resume(process):
    if process is not None:
        try:
            os.kill(process.pid, signal.SIGCONT)
        except OSError:
            print("could not resume process %s. already finished" % process.pid)

def main(args):
    options = get_options(args)
    dbSchema = import_from_file(options.schema)

    if not options.log:
        options.log = os.path.join(options.region, "log_replay_loops")
    outf = open(os.path.join(THIS_DIR, os.path.normpath(options.log)), "w")
    sys.stdout = tools.TeeFile(sys.__stdout__, outf)

    detCmd = [sys.executable, options.schema,
                '--region', options.region,
                '--confFile', options.confFile,
                '--type', 'detector']
    detProcess = Popen(detCmd, stdout=PIPE, stdin=PIPE, stderr=STDOUT)
    print("started detector process %s." % detProcess.pid)
    stop(detProcess)

    simCmd = [sys.executable, options.schema,
                '--region', options.region,
                '--confFile', options.confFile,
                '--type', 'simulation']
    simProcess = Popen(simCmd, stdout=PIPE, stdin=PIPE, stderr=STDOUT)
    print("started simulation process %s." % simProcess.pid)
    stop(simProcess)

    while detProcess and simProcess:
        resume(detProcess)
        if read_until(detProcess, "Duration:") is None:
            print("detector process finished")
            detProcess = None
        stop(detProcess)
        resume(simProcess)
        if read_until(simProcess, "Duration:") is None:
            print("simulation process finished")
            simProcess = None
        stop(simProcess)

    print("finished replaying loops")


if __name__ == "__main__":
    main(sys.argv)
