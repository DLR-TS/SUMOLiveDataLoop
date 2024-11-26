# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    evalDetector.py
# @author  Michael Behrisch
# @author  Yun-Pang Flötteröd
# @date    2007-07-18
"""
Evaluate errors in detector data.
"""

from datetime import datetime
import time

from . import database
from .setting import dbSchema

MAX_FLOW = 2500
MAX_SPEED = {
        'PKW': 250 * (1.0 / dbSchema.EvalDetector.kmhMultiplier),
        'LKW': 120 * (1.0 / dbSchema.EvalDetector.kmhMultiplier)
        }

LENGTH = {'PKW' : 5, 'LKW': 10} # meter

_DELTA = 0.001
_ATTRIBUTE_QUALITY = {"qPKW" : 70, "qLKW" : 10, "vPKW" : 10, "vLKW" : 8, "Date" : 2}
_ATTRIBUTE_QUALITY_NO_LKW = {"qPKW" : 80, "qLKW" : 0, "vPKW" : 18, "vLKW" : 0, "Date" : 2}


def _getDataError(q, v, vMax, l, updateInterval):
    """Returns the error code if the detector data has errors affecting flow and speed."""
    if q == None:
        return 1
    if q < 0.0:
        return 2
    if v != None:
        flowPerHour = q * 3600 / updateInterval.seconds
        if v < 0.0:
            return 2
        if v > 0 and q == 0.0:
            return 5
        if flowPerHour > MAX_FLOW or v > vMax:
            return 7
        if v > 0.0 and flowPerHour > maxFlowPerHour(v, l):
            return 8
    return 0


def maxFlowPerHour(v, l):
    """0.4 * speed in km/h is an empirical safety distance value"""
    return v * 3600 / (v * dbSchema.EvalDetector.kmhMultiplier * 0.4 + l)


def isDataError(errorCode):
    """Returns whether the error code indicates that flow and speed are affected."""
    return errorCode in [1, 2, 3, 4, 5, 7, 8]

def _getSpeedError(q, v, maxLaneSpeed):
    """Returns the error code if the detector data has errors affecting speed only."""
    if q == None or (q > 0.0 and v == None):
        return 1
    if v != None:
        if q > 0.0 and v == 0.0:
            return 6
        if v / maxLaneSpeed > 1.25:
            return 9
    return 0
    
def _nullFloatString(arg):
    """Returns the string "NULL" for a None argument, and the argument
    converted to a string representing a float with two decimals otherwise."""
    if arg == None:
        return "NULL"
    return "%.2f" % arg

def _isEqual(value, other):
    """Compares two floats for equality respecting a DELTA."""
    if value == None:
        return other == None
    if other == None:
        return False
    return abs(value - other) < _DELTA

def is_flow_attr(attr):
    """Whether this attribute holds a flow value"""
    return attr[0] == 'q'

def is_speed_attr(attr):
    """Whether this attribute holds a speed value"""
    return attr[0] == 'v'


def to_type_or_null(value, type):
    """Convert for DB write"""
    if value is None:
        return 'Null'
    else:
        return type(value)

def round_int(x):
    """Round to integer"""
    return int(round(x))


class Data:
    """Represents a data item consisting mainly of speeds and flows."""

    attrs = ('qPKW', 'qLKW', 'vPKW', 'vLKW')
    NO_ORIG_DATA = -1
    FORECAST_DATA = -2

    @classmethod
    def isOrigID(cls, id):
        if dbSchema.Loop.region_choices[0] in ["huainan", "leipzig"]:
            return id is not None and int(id.weekday()) >= 0
        else:
            return id >= 0

    def __init__(self, origID, detectorID, origDate, qKFZ, qLKW, vPKW, vLKW):
        self.origID = origID
        self.detID = detectorID
        self.origDate = origDate
        if qKFZ != None and qLKW != None:
            self.qPKW = qKFZ - qLKW
        else:
            self.qPKW = qKFZ                
        self.qLKW = qLKW
        self.vPKW = vPKW
        self.vLKW = vLKW
        self.errorPKW = 0
        self.errorLKW = 0
        self.fixed = set()
        self.toBeWritten = True

    def hasOrigID(self):
        if dbSchema.Loop.region_choices[0] in ["huainan", "leipzig"]:
            if type(self.origID) == int:
                return self.origID is not None and self.origID >= 0
            else:
                return self.origID is not None and self.origID.weekday() >= 0
        else:
            return self.origID is not None and self.origID >= 0

    def getIfNotFixed(self, attr):
        """Returns the value of the attr if it was not fixed, None otherwise."""
        if not self.hasOrigID() or attr in self.fixed:
            return None
        return getattr(self, attr)

    def fix(self, attr, value, updateInterval):
        """Gives the new value to attr, adds it to the "fixed" set
        and performs consistency checks. The checks are very similar to those
        performed in check(). However, in case of inconsistent flow and speed
        values, only speed is discarded instead of both values.
        Returns true if the value is actually fixed.
        """
        assert(value is not None)
        if is_flow_attr(attr):
            if value < 0:
                return False # error 2
            flowPerHour = value * 3600 / updateInterval.seconds
            if flowPerHour > MAX_FLOW:
                return False # error 7
        else: # value is a speed
            type = attr[1:] # PKW / LKW
            flow = getattr(self, 'q' + type)
            if flow is None:
                # if flow could not be fixed, we don't trust the speed value either
                # note that the attempt to fix flow must come BEFORE the attempt to fix speed
                # (see the order of Data.attrs)
                return False
            if flow == 0: 
                if value > 0:
                    return False # error 5
            else: # flow > 0
                if _getSpeedError(flow, value, MAX_SPEED[type]) > 0:
                    return False # error 6 or 9
                flowPerHour = flow * 3600 / updateInterval.seconds
                if flowPerHour > maxFlowPerHour(value,  LENGTH[type]):
                    return False # error 8
        # no errors
        setattr(self, attr, value)
        self.fixed.add(attr)
        self.toBeWritten = True
        return True


    def unfix(self):
        """Resets all values which have been fixed to None."""
        if len(self.fixed) > 0:
            for attr in self.fixed:
                setattr(self, attr, None)
            self.fixed.clear()
            self.toBeWritten = True

    def check(self, updateInterval, hasLKW=True):
        """Performs the evalDetector checks and sets the error codes.
        Returns True If no errors where encountered.
        """
        noErrors = True
        self.errorPKW = _getDataError(self.qPKW, self.vPKW, MAX_SPEED['PKW'],
                                      LENGTH['PKW'], updateInterval)
        if self.errorPKW > 0:
            self.qPKW = None
            self.vPKW = None
            noErrors = False
        else:
            self.errorPKW = _getSpeedError(self.qPKW, self.vPKW, MAX_SPEED['PKW'])
            if self.errorPKW > 0 or self.qPKW == 0:
                self.vPKW = None
                noErrors = False
        if hasLKW:
            self.errorLKW = _getDataError(self.qLKW, self.vLKW, MAX_SPEED['LKW'],
                                          LENGTH['LKW'], updateInterval)
            if self.errorLKW > 0:
                self.qLKW = None
                self.vLKW = None
                noErrors = False
            else:
                self.errorLKW = _getSpeedError(self.qLKW, self.vLKW, MAX_SPEED['LKW'])
                if self.errorLKW > 0 or self.qLKW == 0:
                    self.vLKW = None
                    noErrors = False
        return noErrors


    def set_hanging(self, is_hanging):
        """invalidate this datapoint if the detector is hanging"""
        if is_hanging:
            self.errorPKW = 4
            self.qPKW = None
            self.vPKW = None
            self.errorLKW = 4
            self.qLKW = None
            self.vLKW = None


    def decomposeErrorCode(self, errorCode, date):
        """Decompose error numbers from the error code."""
        if errorCode:
            if errorCode >= 10000:
                errorCode -= 10000
            else:
                self.origDate = date
            self.errorPKW = errorCode / 100
            self.errorLKW = errorCode % 100
    
    def _getQuality(self, date, hasLKW):
        """Calculates the quality of the item as an integer between 0 and 100."""
        quality = 0
        if (hasLKW):
            qualIter = iter(_ATTRIBUTE_QUALITY.items())
        else:
            qualIter = iter(_ATTRIBUTE_QUALITY_NO_LKW.items())
        for attr, maxQuality in qualIter:
            if attr == "Date":
                if date == self.origDate:
                    quality += maxQuality
            elif getattr(self, attr) != None or\
                 (attr[0] == "v" and getattr(self, "q" + attr[1:]) == 0):
                if attr in self.fixed:
                    quality += maxQuality / 2
                else:
                    quality += maxQuality
        return quality

    def toValues(self, date, hasLKW=True):
        """Returns a array representation suitable for batch SQL insertion"""
        if not self.toBeWritten:
            return None
        assert(self.origID is not None)
        #XXX add column to DB
        #errorCode = 0
        #if date != self.origDate:
        #    errorCode = 10000
        #errorCode += 100 * self.errorPKW + self.errorLKW

        # Warning! these strings will be send directly as SQL query which always carries the
        # risk of string injection attacks. Thus we are extra careful about
        # checking their types
        assert(type(date) == datetime)
        return dbSchema.EvalDetector.toValues(self,
                date,
                to_type_or_null(self.qPKW, round_int), 
                to_type_or_null(self.qLKW, round_int),
                to_type_or_null(self.vPKW, float),
                to_type_or_null(self.vLKW, float),
                int(self._getQuality(date, hasLKW)))
       

    def __eq__(self, other):
        if other == None:
            return False
        return self.detID == other.detID and\
            _isEqual(self.qPKW, other.qPKW) and\
            _isEqual(self.qLKW, other.qLKW) and\
            _isEqual(self.vPKW, other.vPKW) and\
            _isEqual(self.vLKW, other.vLKW)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "<%s %s %s %s>" % (self.qPKW, self.qLKW, self.vPKW, self.vLKW)
