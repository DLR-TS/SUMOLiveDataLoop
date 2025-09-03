#!/usr/bin/env python
# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    main.py
# @author  Michael Behrisch
# @author  Jakob Erdmann
# @date    2007-07-18

"""
Main entry point of the library for time triggered repeated tasks in the DSP simulation setup.
"""

import os, sys, traceback, optparse, time, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timedelta

from . import setting, tools

# loop to to config section
TYPE2SECTION = {
        'simulation' : 'Loop',
        'detector'   : 'Detector',
        'checkdata'  : 'PSM'
        }

def _init(dbSchema, loopDir):

    optParser = optparse.OptionParser()
    optParser.add_option("-r", "--region", dest="region", type="choice",
                         choices=dbSchema.Loop.region_choices,
                         default=dbSchema.Loop.region_choices[0],
                         help="REGION to simulate [default: %default]", metavar="REGION")
    optParser.add_option("-l", "--log", dest="log",
                         help="write log to FILE", metavar="FILE")
    optParser.add_option("-t", "--type", dest="typeOfLoop", type="choice",
                         choices=('checkdata', 'detector', 'simulation'),
                         default=dbSchema.Loop.default_type, help="type of loop [default: %default]")
    optParser.add_option("-c", "--confFile", dest="confFile", type="string",
                         default=dbSchema.Loop.default_config, help="config file for db / timeinterval [default: %default]")
    optParser.add_option("-s", "--scenario", type="string", default="", help="name of the scenario to use")
    optParser.add_option("-b", "--begin", type="string", help="begin time (overrides config setting)")
    optParser.add_option("-e", "--end", type="string", help="end time (overrides config setting)")
    optParser.add_option("-i", "--timeline", type="string", help="time line to use (if any)")
    optParser.add_option("--no-correction", dest="do_correction", default=True,
            action="store_false", help="Skip detector correction (if already handled by another process)")
    optParser.add_option("--clean", default=False, action="store_true", help="clean tables")
    (options, args) = optParser.parse_args()
    
    # Reads the settings and processes them to initialize the loop.
    filename = os.path.join(loopDir, options.confFile) 
    setting.init(dbSchema, filename)

    if args:
        print("Invalid argument.", file=sys.stderr)
        optParser.print_help()
        sys.exit(1)

    setting.setRegion(options.region)
    
    # type of loop mode
    if options.typeOfLoop == "detector":
        from . import correctDetector
        mainFunc = correctDetector.main
        repeatTime = setting.getDetectorOptionMinutes("repeat")
    else:
        from . import simulationRun
        mainFunc = simulationRun.main
        repeatTime = setting.getLoopOptionMinutes("repeat")

    repeatMin = repeatTime.seconds / 60
    # repeat time < 1 day else ...
    if 1440 % repeatMin > 0:
        print("Error! The repeat interval length should be a divider of 1440.")
        sys.exit(1)
    
    setting.startTime =  tools.roundToMinute(setting.getOptionDate("Loop", "starttime", options.begin), repeatTime, tools.ROUND_DOWN)
    setting.endTime = setting.getOptionDate("Loop", "endtime", options.end)
    setting.timeline = options.timeline

    if not options.log:
        options.log = os.path.join(setting.getLoopOption("region"), 
                                            "log_%s_%s_%s.txt" % (options.typeOfLoop, options.scenario,
                                            setting.startTime.strftime("%Y_%m_%d_%H-%M-%S")))
    dirName = os.path.join(loopDir, os.path.dirname(options.log))
    if dirName != '' and not os.path.exists(dirName):
        os.makedirs(dirName)
    outf = open(os.path.join(loopDir, os.path.normpath(options.log)), "w")
    sys.stdout = tools.TeeFile(sys.__stdout__, outf)
    print("Log file: %s TEXTTEST_IGNORE" % options.log)
    
    if setting.startTime.minute % repeatMin > 0:
        setting.startTime += timedelta(minutes=repeatMin-setting.startTime.minute%repeatMin)

    return mainFunc, repeatTime, options.typeOfLoop, options

def _startLoop(mainFunc, repeat, loopType, loopDir, options):
    """
    Starts the loop with the parameters and options read from 
    startConfiguration.
    """
    # delay is used for dealing with offsets between the various scripts
    # detector correction needs to wait for incoming data
    # simulation and checkdata need to wait for correction to finish
    delay = setting.getOptionMinute(TYPE2SECTION[loopType], "delay")
    endTimeReached = False
    secondsToRestart = 60.0
    while not endTimeReached: # When using this line, please activate the "sleep" statement below at the end of this method
    #if True: #This line is a replacement for the "while" statement to leave the indent unchanged
        print("""\
*****************************************************************************
Starting loop

Current time: %s TEXTTEST_IGNORE
Start time:   %s
Will repeat every %s 
*****************************************************************************
Executing first step immediately""" % (datetime.now(), setting.startTime, repeat))
            
        # loop
        try: # ToDo: Make this try block obsolete by holding the called mainFunc responsible for all exception handling
            psmMessage = "&st=1&stDes=Loop started"
            sendMessageToPsm(psmMessage, loopType)
            # STYLE: Gibt es hier einen besonderen Grund warum der Zugriff auf die "main"-Funktionen
            # nicht ueber Vererbung impl. wurde? Ist naemlich ziemlich verschachtelt!
            doContinue = mainFunc(True, False, loopDir, options)
            while doContinue and setting.startTime < setting.endTime:
                startedTime = setting.startTime
                setting.startTime += repeat
                if setting.startTime >= setting.endTime:
                    endTimeReached = True
                waitTime = setting.startTime + delay - datetime.now()
                if tools.dayMinute(setting.startTime) == 0:
                    print("Starting new day")
                    psmMessage = "&st=1&stDes=Loop Run completed, StartedTime=" + str(startedTime) + ", Waited=" + str(waitTime)
                    sendMessageToPsm(psmMessage, loopType)
                    doContinue = mainFunc(False, True, loopDir, options)
                else:
                    if waitTime > timedelta(0):
                        print("Waiting %i seconds till begin." % waitTime.seconds)
                        psmMessage = "&st=1&stDes=Loop Run completed, StartedTime=" + str(startedTime) + ", Waiting=" + str(waitTime)
                        sendMessageToPsm(psmMessage, loopType)
                        time.sleep(waitTime.seconds)
                    else:
                        print("Delayed by %s!" % (-waitTime), 'TEXTTEST_IGNORE')
                        psmMessage = "&st=0&stDes=Loop Run completed, StartedTime=" + str(startedTime) + ", Delayed=" + str(-waitTime)
                        sendMessageToPsm(psmMessage, loopType)
                    doContinue = mainFunc(False, False, loopDir, options)
                sys.stdout.flush()
        except:
            traceback.print_exc(file=sys.stdout)
            print("Loop terminated unexpectedly.")
            psmMessage = "&st=0&stDes=Loop terminated unexpectedly."
            sendMessageToPsm(psmMessage, loopType)
    
        print("""\
*****************************************************************************
Loop ended

Current time: %s TEXTTEST_IGNORE
End time:   %s
*****************************************************************************\
    """ % (datetime.now(), setting.startTime))

        if not endTimeReached:
            print("Trying again, new loop start in %.0f seconds..." % secondsToRestart)
            time.sleep(secondsToRestart) # Please activate this line when using the "while" statement
            print("...restart now:")
        else:
            print("End time reached, exiting.")

def sendMessageToPsm(messageString, loopType):
    if loopType != "checkdata":
        timestring = str(int(float(time.time()) * 1000)) 
        psmPid = setting.getPsmOption("pid" + loopType)
        psmHttp = setting.getPsmOption("http" + loopType)
        if psmHttp != "":
            print("   PSM   ", psmHttp)
            urlstring = "http://" + psmHttp + "/PSM/SetProcessState.jsp?pid=" + psmPid
            urlstring += messageString.replace(" ", "%20")
            urlstring += "&ts=" + timestring
            try:
                urllib.request.urlopen(urlstring)
            except:
                print("Problem output to PSM:")
                traceback.print_exc(file=sys.stdout)

def main(dbSchema, loopDir):
    """
    Calls to initialize and start the loop.
    """
    mainFunc, repeat, loopType, options = _init(dbSchema, loopDir)
    _startLoop(mainFunc, repeat, loopType, loopDir, options)

if __name__ == "__main__":
    print("This is a library not meant for stand-alone execution. Call the main function from a script instead")
