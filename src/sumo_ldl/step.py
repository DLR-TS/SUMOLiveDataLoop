# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    step.py
# @author  Michael Behrisch
# @date    2007-07-18
"""
Helper functions for single steps in detector and simulation loop.
"""
import os, sys, tempfile, traceback
from datetime import datetime, timedelta

from . import setting, tools, database
from .setting import dbSchema

def _checkOutput(lastTime, stdoutFile=None, stderrFile=None):
    """Parses the output files of a step for warnings etc. and outputs the total time."""
    print("step#%s" % setting.step, end=' ')
    errorCount = 0
    warningCount = 0
    sizeSum = 0
    hadOK = False
    for outFile in [stdoutFile, stderrFile]:
        if outFile:
            sizeSum += os.path.getsize(outFile)
            for line in open(outFile):
                line = line.lower()
                if line.find("error") > -1 or line.find("exception") > -1:
                    errorCount += 1
                if line.find("warning") > -1:
                    warningCount += 1
                if line.find("ok") > -1 or line.find("simulation ended at time") > -1:
                    hadOK = True
    if errorCount > 0:
        print("had errors,")
    elif warningCount > 0:
        print("had warnings,")
    elif stderrFile and os.path.getsize(stderrFile) > 0:
        print("had non-empty stderr,")
    elif hadOK or sizeSum == 0:
        print("ok,")
    else:
        print("unknown status,")
    totalTime = datetime.now() - lastTime
    print("...needed %s (%s)" % (totalTime, setting.databaseTime), 'TEXTTEST_IGNORE')
    setting.databaseTime = timedelta(0)
    setting.step += 1
    print("- " * 39)

def systemStep(comment, command, checkDir, suffix):
    """Executes a step which involves an os.system call."""
    lastTime = datetime.now()
    exe = os.path.basename(command.split()[0])
    if exe.endswith(".exe"):
        exe = exe[:-4]
    checkOut = os.path.join(checkDir, "%02i%s_%s.txt" % (setting.step, exe, suffix))
    checkErr = os.path.join(checkDir, "%02i%sError_%s.txt" % (setting.step, exe, suffix))
    print("step#%s" % setting.step)
    print(" (%s)" % comment)
    print(" Call:", command, 'TEXTTEST_IGNORE')
    print(" redirecting stdout to %s and stderr to %s" % (checkOut, checkErr), 'TEXTTEST_IGNORE')
    os.system(command + " > %s 2> %s" % (checkOut, checkErr))
    _checkOutput(lastTime, checkOut, checkErr), 'TEXTTEST_IGNORE'

def pythonStep(comment, function, args, checkDir=None, suffix=None):
    """Executes a step which is a python function call."""
    lastTime = datetime.now()
    print("step#%s" % setting.step)
    print(" (%s)" % comment)
    checkErr = None
    temp = None
    if checkDir:
        checkErr = os.path.join(checkDir, "%02i%s_%s.txt" % (setting.step, function.__name__, suffix))
        sys.stderr = open(checkErr, 'w')
    else:
        fd, checkErr = tempfile.mkstemp()
        os.close(fd)
        temp = open(checkErr, 'w')
        sys.stderr = tools.TeeFile(sys.stdout, temp)
    result = None
    try:
        print(" Call: %s%s" % (function.__name__, args), 'TEXTTEST_IGNORE')
        if not temp:
            print(" redirecting stderr to %s" % checkErr, 'TEXTTEST_IGNORE')
        result = function(*args)
        if temp:
            temp.close()
        else:
            sys.stderr.close()
    except KeyboardInterrupt:
        if temp:
            print("Interrupted! Temporary output file left at %s." % checkErr)
        else:
            print("Interrupted!")
        raise
    except:
        if temp:
            print("Exception caught! Temporary output file left at %s." % checkErr)
        else:
            print("Exception caught!")
        traceback.print_exc()
        setting.errorOnLastRun = True
    _checkOutput(lastTime, stderrFile=checkErr)
    try:
        if temp:
            os.remove(temp.name)
    except:
        print("Error cleaning temp! Temporary output file left at %s." % checkErr)
        traceback.print_exc()
    sys.stderr = sys.__stderr__
    return result
