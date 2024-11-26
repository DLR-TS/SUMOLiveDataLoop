# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    tools.py
# @author  Michael Behrisch
# @date    2007-07-18
"""
Helper classes and functions for the simulation setup.
"""
import os
import sys
import math
import time
from datetime import timedelta
import operator

class TeeFile:
    """A helper class which allows simultaneous writes to several files"""
    def __init__(self, *files):
        self.files = files
    def write(self, txt):
        """Writes the text to all files"""
        for fp in self.files:
            fp.write(txt)
    def flush(self):
        """flushes all file contents to disc"""
        for fp in self.files:
            fp.flush()
            if fp != sys.__stdout__:
                os.fsync(fp)


ROUND_UP = 1
ROUND_DOWN = 2
ROUND_HALF_UP = 3

def dayMinute(time):
    """Returns the minute of the day as given by time, a number in [0, 1440)."""
    return time.hour * 60 + time.minute

def daySecond(time, begin=-1):
    """Returns the second of the day as given by time, a number in [0, 24*3600).
       If begin is given, the result is increment in steps of whole days until
       it is larger than the value of begin."""
    result = time.hour * 3600 + time.minute * 60 + time.second
    while result < begin:
        result += 24 * 3600
    return result

def roundToMinute(date, interval=timedelta(minutes=1), rounding=ROUND_HALF_UP):
    """Rounds the date to the next full minute or minute interval.
    Also works if interval is not integer minutes but assumes that it is a fraction
    of 24 * 60 minutes
    """
    assert(24 * 3600 % interval.seconds == 0)
    seconds = daySecond(date)
    if seconds % interval.seconds == 0:
        return date
    # handle rounding direction
    if rounding == ROUND_DOWN:
        pass
    elif rounding == ROUND_HALF_UP:
        seconds += interval.seconds / 2
    elif rounding == ROUND_UP:
        seconds += interval.seconds 
    else:
        raise ValueError
    # trunkcate
    #seconds = seconds % (24 * 3600) # don't change the day
    resultSeconds = interval.seconds * int(seconds / interval.seconds)
    result = date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=resultSeconds)
    return result


def getIntervalEndsBetween(start, end, intervalLength):
    result = []
    while start < end:
        start += intervalLength
        result.append(start)
    return result


# decorator for timing a function
def benchmark(func):
    def benchmark_wrapper(*args, **kwargs):
        started = time.time()
        now = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime())
        print(('function %s called at %s' % (func.__name__, now)))
        sys.stdout.flush()
        result = func(*args, **kwargs)
        print(('function %s finished after %f seconds' % (func.__name__, time.time() - started)))
        sys.stdout.flush()
        return result
    return benchmark_wrapper


def geh(m,c):
    """Error function for hourly traffic flow measures after Geoffrey E. Havers"""
    if m+c == 0:
        return 0
    else:
        return math.sqrt(2 * (m-c) * (m-c) / float(m+c))


class Table:
    # class for storing table and column name strings
    def __init__(self, name, **columns):
        self.name = name
        for attr, value in list(columns.items()):
            setattr(self, attr, value)

    def __str__(self):
        return self.name

def noneToNull(val):
    if val is None:
        return 'Null'
    else:
        return val


def safeBinaryOperator(binaryOperator):
    """works sensibly with None values"""
    def resultFun(a,b):
        if a is not None and b is not None:
            return binaryOperator(a,b)
        else:
            return None
    return resultFun

SAFE_ADD = safeBinaryOperator(operator.add)
SAFE_SUB = safeBinaryOperator(operator.sub)
SAFE_MUL = safeBinaryOperator(operator.sub)
SAFE_DIV = safeBinaryOperator(operator.truediv)

def reversedMap(map):
    """return reversed map assuming input is a bijection"""
    return dict([(v,k) for k,v in list(map.items())])
