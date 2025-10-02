# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    setting.py
# @author  Michael Behrisch
# @date    2007-10-22
"""
Configuration interface for the Delphi simulation setup.
"""
import os,sys
from configparser import ConfigParser, NoOptionError
from datetime import datetime, timedelta

THIS_DIR = os.path.dirname(__file__)
_CONFIG = ConfigParser({"starttime": datetime.now().strftime("%Y-%m-%d %H:%M")})

# global variables
startTime = None
endTime = None
timeline = None
scenarioID = None
databaseTime = timedelta(0)
step = 1
errorOnLastRun = False
lastCleanup = datetime.min
edges = None
loopDir = None
updateIntervals = None # list of update intervals to handle in correct/aggregate detector (in seconds)
# this is the module responsible for abstracting from different database schemas (delphi, mobilind, aim)
dbSchema = None

def init(schema, filename="delphi.cfg"):
    global loopDir
    global dbSchema
    dbSchema = schema
    for section in _CONFIG.sections():
        _CONFIG.remove_section(section)
    configFile = open(filename)
    loopDir = os.path.dirname(configFile.name)
    _CONFIG.read_file(configFile)

def _checkSubOption(section, option):
    if _CONFIG.has_option("Loop", "region"):
        subOption = option + "." + _CONFIG.get("Loop", "region")
        if _CONFIG.has_option(section, subOption):
            return subOption
    return option

def hasOption(section, option):
    return _CONFIG.has_option(section, _checkSubOption(section, option))

def getOption(section, option):
    if not hasOption(section, option):
        return ""
    return _CONFIG.get(section, _checkSubOption(section, option))

def getOptionBool(section, option):
    return _CONFIG.getboolean(section, _checkSubOption(section, option))

def getOptionMinute(section, option):
    return timedelta(minutes=_getOptionFloat(section, option))

def getOptionDate(section, option, override=None):
    if override:
        return datetime.strptime(override, "%Y-%m-%d %H:%M")

    date = getOption(section, option)
    if date[0] == '-':
        hour, minute = date[1:].split(":")
        delta = timedelta(hours=int(hour), minutes=int(minute))
        return datetime.now() - delta
    return datetime.strptime(date, "%Y-%m-%d %H:%M")

def getOptionInt(section, option):
    return _CONFIG.getint(section, _checkSubOption(section, option))

def _getOptionFloat(section, option):
    return _CONFIG.getfloat(section, _checkSubOption(section, option))

def getDetectorOption(option):
    return getOption("Detector", option)

def getDetectorOptionBool(option, default=False):
    if not hasOption("Detector", option):
        return default
    return getOptionBool("Detector", option)

def getDetectorOptionMinutes(option):
    return getOptionMinute("Detector", option)

def setRegion(region):
    if not _CONFIG.has_section("Loop"):
        _CONFIG.add_section("Loop")
    _CONFIG.set("Loop", "region", region)

def getLoopOption(option):
    return getOption("Loop", option)

def getLoopOptionMinutes(option):
    return getOptionMinute("Loop", option)

def getLoopOptionBool(option):
    return _CONFIG.getboolean("Loop", _checkSubOption("Loop", option))

def getLoopOptionList(option):
    option = _checkSubOption("Loop", option)
    return [s.strip() for s in _CONFIG.get("Loop", option).split(",")]

def getOSDependentLoopOptionPath(option):
    try:
        optionValue = _CONFIG.get("Loop", _checkSubOption("Loop", option + "." + os.name))
    except NoOptionError:
        optionValue = _CONFIG.get("Loop", _checkSubOption("Loop"))
    return os.path.join(loopDir, optionValue)

def getLoopOptionPathList(option):
    dirList = []
    option = _checkSubOption("Loop", option)
    for s in _CONFIG.get("Loop", option).split(","):
        dirList.append(os.path.abspath(os.path.join(loopDir, s.strip())))
    return dirList

def getPsmOption(option):
    """
    Read all options needed for the PSM tool.
    """
    return getOption("PSM", option)
