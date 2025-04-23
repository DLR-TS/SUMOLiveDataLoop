#!/usr/bin/env python
"""
@file    checkData.py
@author  Matthias.Wagner@dlr.de
@date    2011-08-01
@version $Id: checkData.py 3855 2014-08-19 07:24:02Z erdm_ja $

Test file to check for recently incoming data

Copyright (C) 2011 DLR/FS, Germany
All rights reserved
"""
import os, sys, glob, datetime, optparse, time, traceback, urllib
try:
    import psycopg2 as pgdb
except ImportError:
    import pg, pgdb
from ConfigParser import NoOptionError
from optparse import OptionParser
import database, setting

def init(loopDir):
    # ToDo: Clean up the optParser section
    optParser = optparse.OptionParser()
    optParser.add_option("-r", "--region", dest="region", type="choice",
                         choices=('koeln', 'muenchen', 'citykoeln', 'citymuenchen'),
                         default="muenchen",
                         help="REGION to simulate [default: %default]", metavar="REGION")
    optParser.add_option("-l", "--log", dest="log",
                         help="write log to FILE", metavar="FILE")
    optParser.add_option("-t", "--type", dest="type", type="choice",
                         choices=('checkdata', 'detector', 'simulation'),
                         default="simulation", help="type of loop [default: %default]")
    optParser.add_option("-c", "--confFile", dest="confFile", type="string",
                         default="delphi.cfg", help="config file for db / timeinterval [default: %default]")
    (options, args) = optParser.parse_args()

    """
    Reads the settings.
    """
    filename = os.path.join(loopDir, options.confFile)
    setting.init(filename)
    setting.setRegion(options.region)

# type -> (ConfigSection, ConfigEntry)
REPEAT_INTERVAL_ENTRY = {
        'fcd'           : ('Detector', 'aggregateFCD'),
        'loop'          : ('Detector', 'repeat'),
        'fusion'        : ('Detector', 'repeat'),
        'extrapolation' : ('Detector', 'repeat'),
        'simulation'    : ('Loop', 'repeat'), 
        }
def get_repeat_interval(type):
    section, entry = REPEAT_INTERVAL_ENTRY[type]
    return int(setting.getOption(section, entry))

# type -> String to use for PSM
PSM_STRING = {
        'fcd'           : "FCD",
        'loop'          : "Loop",
        'fusion'        : "Fusion",
        'extrapolation' : "Extrapolation",
        'simulation'    : "Simulation", 
        }
def checkData():
    print 'test aggregation / simulation'
    try:
        dataTypes = setting.getLoopOptionList("checkedTypes")
    except NoOptionError:
        print "Warning: Option \"checkedTypes\" not found in config file."
        dataTypes = ["fcd", "loop", "fusion", "simulation"]
    try:
        conn = database.createDatabaseConnection()    
    
        region = setting.getOption("Loop", "region")
        psmState = "1"
        psmMessage = ""
        for type in REPEAT_INTERVAL_ENTRY.keys():
            repeatInterval = get_repeat_interval(type)
            if type in dataTypes and repeatInterval > 0:
                checkInterval = 2 * repeatInterval
                print repeatInterval, checkInterval
                state, time = checkDataType (conn, type, checkInterval, region)
                psmMessage += createPsmMessage(PSM_STRING[type], state, time)
                if state <= 0:
                    psmState = "0"
        conn.close()
    except:
        psmState = "0"
        psmMessage = ""
        print "Problem in checkData, check if the database is running and accessible."
        traceback.print_exc(file=sys.stdout)

    psmMessage = "&st=" + psmState + "&stDes=" + psmMessage
    print psmMessage
    sendMessageToPsm(psmMessage, "checkData")

def checkDataType(conn, type, timeout, region):
    """
    type: Data type (fcd, loop, fusion, simulation); timeout in minutes
    """
    returnState = 0
    print "Checking %s data in %s:" % (type, region)
    rows = database.execSQL(conn, """SELECT max(traffic_time) FROM traffic WHERE traffic_type = '%s'""" % type)
    time = database.as_time(rows[0][0])
    if time is None:
        print ' No %s data available.' % type
        returnState = -1
    else:
        diffTime = datetime.datetime.now() - time
        if diffTime > datetime.timedelta(minutes=timeout):
            print "%s Late! (%s)" % (type, time)
        else:
            print "%s OK (%s)" % (type, time)
            returnState = 1
    return returnState, time

def createPsmMessage(type, state, lastTime):
    if lastTime is not None:
        lastTime = lastTime.strftime("%Y%m%dT%H%M")
    if state == -1:
        psmMessage = "%s:N/A" % type
    elif state == 1:
        psmMessage = "%s:%s" % (type, lastTime)
    else:
        psmMessage = "!%s:%s!" % (type, lastTime)
    return psmMessage

def sendMessageToPsm(messageString, loopType):
    timestring = str(int(float(time.time()) * 1000))
    try:
        psmPid = setting.getPsmOption("pid" + loopType)
        psmHttp = setting.getPsmOption("http" + loopType)
    except NoOptionError:
        print "Warning: No PSM parameters found for checkData %s." % setting.getLoopOption("region")
        return
    urlstring = "http://" + psmHttp + "/PSM/SetProcessState.jsp?pid=" + psmPid
    urlstring += messageString.replace(" ", "%20")
    urlstring += "&ts=" + timestring
    try:
        urllib.urlopen(urlstring)
    except:
        traceback.print_exc(file=sys.stdout)

def main(isFirst, beginNewDay, loopDir, options):
    """
    Arguments are not used, for compatibility only.
    """
    checkData()
    return True

if __name__ == "__main__":
    optParser = OptionParser(usage="usage: %prog [options] <datatypes>")
    (options, args) = optParser.parse_args()
    if len(args) == 0:
        optParser.print_help()
        sys.exit()
    sys.exit("currently not working. talk to the maintainer")
    init(loopDir)
    main(True, False, '')
