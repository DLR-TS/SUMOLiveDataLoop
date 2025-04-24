# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    correctDetector.py
# @author  Michael Behrisch
# @date    2007-09-25
"""
Corrects and aggregates detector data in a specified interval.
Usually it is called from loop.py.
"""
import sys
import os
import pickle, csv
from datetime import datetime, timedelta
from collections import defaultdict

from numpy.polynomial import Polynomial

from . import aggregateData
from . import fusion
from . import correctVisual
from . import setting, database
from .setting import hasOption, getDetectorOption, getDetectorOptionBool, getDetectorOptionMinutes, getLoopOptionMinutes, getLoopOption, getLoopOptionBool, getOptionBool
from .setting import dbSchema
from .database import as_time, as_interval
from .tools import roundToMinute, ROUND_DOWN, ROUND_UP
from .step import pythonStep
from .evalDetector import Data, isDataError
from . import extrapolation

_DEBUG = False
_DEBUG_QUALITY = False

# this constant is used in polynomialFix() when fixing gaps by interpolation and forecasting by extrapolation
# gaps beyond this size should be fixed with the help of historical data
MAX_GAP_TIME = timedelta(minutes=30)

if _DEBUG:
    from tools import benchmark
    # subset of detectors to correct (default is all)
    DETECTOR_FILTER = "AND i.induction_loop_id = 'WB0206-8'"    #yp
else:
    # benchmark decorator disabled by default
    benchmark = lambda x:x 
    DETECTOR_FILTER = ""

class DataWindow:
    """ Helper class holding values which are saved between multiple iterations with a fixed update interval.
    The class holds data for a time window which moves forward over time.
    """
    def __init__(self):
        self.previousEvaluation = None
        self.updateInterval = None
        self.zeroIndexTime = None

    @benchmark
    def reset(self, conn, updateInterval):
        """Resetting the values on a new run."""
        self.origData = set()
        self.updateInterval = updateInterval
        self.zeroIndexTime = None # in seconds
        # main data store
        # detector_id -> [Data0, Data1, ...] 
        # list indices correspond to timestamps 
        # [zeroIndexTime, zeroIndexTime + updateInterval.seconds, zeroIndexTime + 2 * updateInterval.seconds, ...]
        # see dateToIndex()
        self.data = {} 
        self.previousEvaluation = None
        ACTIVE_DETECTOR_FILTER = DETECTOR_FILTER
        if dbSchema.Loop.region_choices[0] == "leipzig":
            ACTIVE_DETECTOR_FILTER= "AND i.id IN (SELECT distinct det_id FROM %s)" % dbSchema.Tables.induction_loop_data

        rows = database.execSQL(conn, "SELECT %s FROM %s i WHERE i.%s = %s %s" % (     
            dbSchema.Tables.induction_loop.induction_loop_id,
            dbSchema.Tables.induction_loop,
            dbSchema.Tables.induction_loop.loop_interval,
            updateInterval.seconds, 
            ACTIVE_DETECTOR_FILTER))
        for row in rows:
            self.data[row[0]] = []

    def new_quality_evaluation(self, now, evaluationinterval):
        """Decide whether quality needs to be evaluated"""
        if (self.previousEvaluation is None or 
                self.previousEvaluation <= (now - evaluationinterval)):
            self.previousEvaluation = now
            return True
        else:
            return False

    @benchmark
    def prepare_dataLists(self, newZeroTime, endTime):
        """Ensure that the dataList contains entries between zeroTime and endTime
        and that any existing data is located at the right indices"""
        if self.zeroIndexTime is None:
            self.zeroIndexTime = newZeroTime # arbitrary 
        assert(self.zeroIndexTime <= newZeroTime)
        keepStart = dateToIndex(newZeroTime)
        keepEnd = dateToIndex(endTime)
        for det, dataList in self.data.items():
            oldEnd = len(dataList)
            extendBy = max(0, keepEnd - oldEnd)
            self.data[det] = dataList[keepStart:keepEnd] + extendBy * [None]
        self.zeroIndexTime = newZeroTime

    def enumerate_with_time(self, dataList, startIndex=0):
        time = self.zeroIndexTime + self.updateInterval * startIndex
        for index, data in enumerate(dataList[startIndex:]):
            yield index + startIndex, time, data
            time += self.updateInterval

    def has_more_data_after(self, conn, time):
        """Whether there is more detector data after time (i.e. when 
        re-running on historical data and there is a large gap)"""
        # XXX not yet implemented (but probably not important either
        # this is only for early stopping when running on historical data
        # however the stopping time should be set in the config file anyway
        return True

    def __repr__(self):
        lines = []
        for det, dataList in self.data.items():
            lines.append("datapoints for detector '%s'" % det)
            for index, time, data in self.enumerate_with_time(dataList):
                lines.append("%5i: %s, %s" % (index, time, data))
        return os.linesep.join(lines)


# map from updateIntervals to DataWindow classes
DATA_WINDOWS = defaultdict(lambda: DataWindow())
# DataWindow for the updateInterval currently being processed
_GLOBALS = None

def dateToIndex(date):
    """Converts a datetime into an index in the data list."""
    return (date - _GLOBALS.zeroIndexTime).seconds // _GLOBALS.updateInterval.seconds
                
def fixedData(origID, detectorID, date, qPKW, qLKW, vPKW, vLKW, errorCode):
    """
    Returns a data set which was already fixed previously and was reread
    from the database. The error code from the database is used to 
    determine which parts of the data set have been fixed.
    """ 
    dat = emptyData(detectorID, origID)
    dat.qPKW = qPKW
    dat.qLKW = qLKW
    dat.vPKW = vPKW
    dat.vLKW = vLKW
    dat.decomposeErrorCode(errorCode, date)
    if not origID:
        dat.fixed = set(["qPKW", "qLKW", "vPKW", "vLKW"])
    else:
        if isDataError(dat.errorPKW) and qPKW != None:
            dat.fixed.add("qPKW")
        if dat.errorPKW > 0 and vPKW != None:
            dat.fixed.add("vPKW")
        if isDataError(dat.errorLKW) and qLKW != None:
            dat.fixed.add("qLKW")
        if dat.errorLKW > 0 and vLKW != None:
            dat.fixed.add("vLKW")
    return dat

def emptyData(detectorID, origID):
    """Returns an empty data set containing only "None" values."""
    dat = Data(origID, detectorID, None, None, None, None, None)
    if origID == Data.FORECAST_DATA:
        dat.toBeWritten = False # only write forecasts if they contain data
    return dat

def fixDate(detData, date, ignore):
    """
    Fixes the date of a given dataset by moving it at most 1 minute
    to a free position in the time ordered list of datasets.
    The return value may be a non valid index because shifting
    into the future may exceed the length of the data list.
    """
    dateIndex = dateToIndex(date)
    fixedDateIndex = dateIndex
    if dateIndex < len(detData) and detData[dateIndex]:
        prevDateIndex = dateIndex - 1
        if detData[prevDateIndex]:
            if detData[dateIndex].origDate == date:
                nextDateIndex = dateIndex + 1
                if nextDateIndex < len(detData) and detData[nextDateIndex]:
                    wasInterrupted = True
                    for offset in range(2, 7):
                        if offset > dateIndex:
                            break
                        if detData[dateIndex - offset]:
                            wasInterrupted = False
                            break
                    if wasInterrupted:
                        detData[prevDateIndex] = None
                        detData[dateIndex] = None
                        detData[nextDateIndex] = None
                        ignore.append(date)
                    else:
                        fixedDateIndex = nextDateIndex
                else:
                    fixedDateIndex = nextDateIndex
        else:
            detData[prevDateIndex] = detData[dateIndex]
    return fixedDateIndex


def is_hanging(detData, dateToCheck):
    """
    Checks whether the detector is hanging (reports the same values)
    for a fixed time interval.
    """
    offset = 5 # minutes
    startIndex = dateToCheck - offset
    if startIndex < 0:
        return False # not enough datapoints
    # return False if we encounter varying values for any attribute
    for attr in ["qPKW", "qLKW", "vPKW", "vLKW"]:
        value = detData[dateToCheck].getIfNotFixed(attr) 
        if value != None and value > 0:
            for i in range(startIndex, startIndex + offset):
                if not detData[i] or value != detData[i].getIfNotFixed(attr):
                    return False
        else:
            return False
    return True


def find_gaps(detData, attr, start, end):
    assert(start >= 0)
    assert(end <= len(detData))
    index = start
    while index < end:
        # find start of gap
        while index < end and getattr(detData[index], attr) is not None:
            index += 1
        if index >= end:
            return
        gapStart = index
        # find end of gap
        while index < end and getattr(detData[index], attr) is None:
            index += 1
        gapEnd = index
        yield gapStart, gapEnd


def valid_indices_and_data(detData, attr, start, end):
    """return a list of indices and valid values for the given attribute and range"""
    start = max(start, 0)
    end = min(end, len(detData))
    indices = []
    values = []
    for i in range(start, end):
        if detData[i] is not None:
            value = detData[i].getIfNotFixed(attr)
            if value is not None:
                indices.append(i)
                values.append(value)
    return indices, values


def polynomialFix(detData, start, end, fix_counts, forecast):
    """
    Fixes gaps in the data by fitting a polynomial to the surrounding data
    and computing its value at the missing positions
    """
    DEGREE = 1 # fit a linear function
    MAX_GAP = MAX_GAP_TIME.seconds / _GLOBALS.updateInterval.seconds
    for attr in Data.attrs:
        for gapStart, gapEnd in find_gaps(detData, attr, start, end):
            x = y = None
            size = gapEnd - gapStart
            if forecast:
                if size > MAX_GAP:
                    sys.stderr.write("WARNING: forecast interval exceeds %s\n" % MAX_GAP_TIME)
                # require size valid points in window of 2*size before gap
                # with less support we can close a smaller gap
                x, y = valid_indices_and_data(detData, attr, gapStart - 2*size, gapStart)
                support = len(x)
                while support < size and size > 0:
                    size -= 1
                    # throw away support earlier than gapStart - 2*size
                    first_valid_index = gapStart - 2*size
                    while support > 0 and x[-support] < first_valid_index:
                        support -= 1
                gapEnd = gapStart + size
                if support < 2: # do not extrapolate from a single data point
                    x = None
                    y = None

            else: # interpolation
                if size <= MAX_GAP: 
                    # require size/2 valid points in window of size before and after gap
                    required = max(size/2, 1) 
                    x_before, y_before = valid_indices_and_data(detData, attr, gapStart - size, gapStart)
                    x_after, y_after = valid_indices_and_data(detData, attr, gapEnd, gapEnd + size)
                    if len(x_before) >= required and len(x_after) >= required:
                        x = x_before + x_after
                        y = y_before + y_after
                else:
                    # XXX use historical data
                    pass

            if x:
                # compute replacement
                coeffs = Polynomial.fit(x, y, DEGREE).convert().coef
                for x in range(gapStart, gapEnd):
                    value = sum([coeffs[p] * x ** p for p in range(len(coeffs))])
                    if detData[x].fix(attr, value, _GLOBALS.updateInterval):
                        fix_counts[attr]+= 1


@benchmark
def evalDetectorQuality(conn, intervalStart, intervalEnd, updateInterval):
    """
    Calculate a quality value for every detector and every group based on
    the detector data in the given interval and write it to the database.
    """
    length = (intervalEnd - intervalStart)
    totalEntries = (length.seconds + length.days * 24 * 3600) / _GLOBALS.updateInterval.seconds 
    query = """
        SELECT c.%s, 
               SUM(CASE WHEN c.quality>=70 THEN 1 ELSE 0 END),
               SUM(CASE WHEN c.quality>=98 THEN 1 ELSE 0 END),
               SUM(CASE WHEN c.q_pkw>0 OR c.q_lkw>0 THEN 1 ELSE 0 END),
               SUM(CASE WHEN c.q_pkw is not Null OR c.q_lkw is not Null THEN 1 ELSE 0 END),
               SUM(c.quality)
        FROM %s c
        INNER JOIN %s i ON c.%s = i.%s
        WHERE i.%s = %s
        AND c.data_time >= %s '%s' 
        AND c.data_time < %s '%s'
        GROUP BY c.%s""" % (
                dbSchema.Tables.corrected_loop_data.induction_loop_id,
                dbSchema.Tables.corrected_loop_data,
                dbSchema.Tables.induction_loop,
                dbSchema.Tables.corrected_loop_data.induction_loop_id,
                dbSchema.Tables.induction_loop.induction_loop_id,
                dbSchema.Tables.induction_loop.loop_interval,
                updateInterval.seconds,
                setting.dbSchema.AggregateData.getTimeStampLabel(),
                intervalStart,
                setting.dbSchema.AggregateData.getTimeStampLabel(),
                intervalEnd,
                dbSchema.Tables.corrected_loop_data.induction_loop_id)
    rows = database.execSQL(conn, query)

    if dbSchema.Loop.region_choices[0] != "huainan":
        # since AVG does not work in oracle, delay is not calculated here. 
        # need to revise the codes when using orcale
        query_delay = """
            SELECT %s, AVG(%s - data_time) FROM %s 
            WHERE data_time >= %s '%s' 
            AND data_time < %s '%s'
            GROUP BY %s""" % (
                    dbSchema.Tables.induction_loop_data.induction_loop_id,
                    dbSchema.Tables.induction_loop_data.database_time,
                    dbSchema.Tables.induction_loop_data,
                    setting.dbSchema.AggregateData.getTimeStampLabel(),
                    intervalStart,
                    setting.dbSchema.AggregateData.getTimeStampLabel(),
                    intervalEnd,
                    dbSchema.Tables.induction_loop_data.induction_loop_id)
        det_delay = dict(database.execSQL(conn, query_delay))
        idName = 'induction_loop_id'
    else:
        idName = 'stationary_sensor_id'

    qualitySum = 0
    delaySum = timedelta()
    delayRows = 0
    delay_info = 0.
    status_data = []
    # leipzig.
    #if not getOptionBool("Database", "postgres"):
    deleteData = []
    for row in rows:
        det, num_q70, num_q98, positive_entries, num_entries, qSum = row
        if dbSchema.Loop.region_choices[0] in ["huainan", "leipzig"]:
            avgDelay = None
        else:
            avgDelay = det_delay.get(det, None)
        # determine quality and status
        # if num_entries is too low, we know that the detector cannot be trusted
        # however, if there are too few positive entries it is suspicious as well
        # (given the knowledge that these detectors are placed on busy roads)
        quality = qSum / totalEntries
        if num_entries >= totalEntries / 2:
            if num_q98 >= totalEntries * 0.95:
                category = "I"
                status = 'ok'
            elif num_q70 >= totalEntries * 0.95:
                category = "II"
                status = 'breaks'
            elif num_q70 >= totalEntries * 0.75:
                category = "III"
                status = 'irregular'
            else:
                category = "IV"
                status = 'sporadic'
        else:
            category = "V"
            status = 'defect'
        # aggregate delay
        if avgDelay is not None:
            delaySum += as_interval(avgDelay)
            delayRows += 1
            avgDelay = "'%s'" % avgDelay
        # deletet previous data    
        # comment it out for Leipzig
        #if not getOptionBool("Database", "postgres"):
        deleteData.append("('%s', TIMESTAMP '%s')" % (det, intervalEnd))
        # extend sql command
        status_data.append((det, intervalEnd, status, quality, avgDelay))
        if _DEBUG_QUALITY:
            print(intervalEnd, row, quality) 
        qualitySum += quality
    if qualitySum == 0:
        print("Warning! All detectors have insufficient quality.  Writing to DB suspended", file=sys.stderr)
    else:
        if _DEBUG_QUALITY:
            print(command)
        else:

            if delayRows > 0:
                delay_info = delaySum / delayRows
            print("Updating Operating status for %s detectors with average quality %s and average delay %s" % (
                    len(rows), qualitySum / len(rows), delay_info))
            # delete previous entries 
            deleteCommand = """DELETE FROM %s
                         WHERE status_time = %s '%s'""" % (
                                dbSchema.Tables.operating_status,
                                dbSchema.AggregateData.getTimeStampLabel(),
                                intervalEnd)
            database.execSQL(conn, deleteCommand, True)

            # write new
            # this will not work when using postgres
            command = "INSERT INTO %s(%s, status_time, operating_status, quality, delay) VALUES (:1, :2, :3, :4, :5)" % (
                              dbSchema.Tables.operating_status, idName)
            database.execSQL(conn, command, True, manySet = status_data)

            group_status_query = """
                INSERT INTO %s(%s, status_time, operating_status, quality, delay)
                SELECT i.%s, status_time, operating_status, MIN(quality), MAX(delay)
                FROM %s o, %s i
                WHERE o.%s = i.%s AND status_time = %s '%s'
                GROUP BY i.%s, status_time,
                operating_status""" % (
                    dbSchema.Tables.operating_status,
                    dbSchema.Tables.induction_loop.induction_loop_group_id,
                    dbSchema.Tables.induction_loop.induction_loop_group_id,
                    dbSchema.Tables.operating_status,
                    dbSchema.Tables.induction_loop,
                    dbSchema.Tables.operating_status.induction_loop_id,
                    dbSchema.Tables.induction_loop.induction_loop_id,
                    dbSchema.AggregateData.getTimeStampLabel(),
                    intervalEnd,
                    dbSchema.Tables.induction_loop.induction_loop_group_id)
            #print group_status_query
            group_status_query = database.execSQL(conn, group_status_query, True)


@benchmark
def load_previous_corrections(conn, start, end, updateInterval):
    rows = database.execSQL(conn, """
        SELECT c.%s, c.%s, c.data_time,
               c.q_pkw, c.q_lkw, c.v_pkw, c.v_lkw
        FROM %s c, %s i
        WHERE c.data_time >= %s '%s' AND c.data_time < %s '%s' 
        AND c.%s = i.%s 
        AND i.%s = %s
        %s
        ORDER BY c.data_time""" % (
            dbSchema.Tables.corrected_loop_data.original_data_id,
            dbSchema.Tables.corrected_loop_data.induction_loop_id,
            dbSchema.Tables.corrected_loop_data,
            dbSchema.Tables.induction_loop,
            setting.dbSchema.AggregateData.getTimeStampLabel(), start,
            setting.dbSchema.AggregateData.getTimeStampLabel(), end,
            dbSchema.Tables.corrected_loop_data.induction_loop_id,
            dbSchema.Tables.induction_loop.induction_loop_id,
            dbSchema.Tables.induction_loop.loop_interval,
            updateInterval.seconds,
            DETECTOR_FILTER))
    # XXX also load errorCode from DB
    errorCode = None
    for id, det, time, qPKW, qLKW, vPKW, vLKW in rows:
        if Data.isOrigID(id):
            index = dateToIndex(database.as_time(time))
            _GLOBALS.data[det][index] = fixedData(id, det, time, qPKW, qLKW, vPKW, vLKW, errorCode)


@benchmark
def storeOrigData(conn, correctStart, updateInterval):
    selectPara = "d.q_kfz, d.q_lkw, d.v_pkw, d.v_lkw"
    if dbSchema.Loop.region_choices[0] == "huainan":
        selectPara = "d.q_motor_vehicle, 0, d.s_motor_vehicle, 0"
    rows = database.execSQL(conn, """
        SELECT d.%s, d.data_time, %s
        FROM %s d, %s i
        WHERE d.%s = i.%s
        AND i.%s = %s
        AND d.data_time >= %s '%s'
        AND d.data_time < %s '%s' 
        %s
        ORDER BY d.data_time""" % (
            dbSchema.Tables.induction_loop_data.induction_loop_id,
            selectPara,
            dbSchema.Tables.induction_loop_data,
            dbSchema.Tables.induction_loop,
            dbSchema.Tables.induction_loop_data.induction_loop_id,
            dbSchema.Tables.induction_loop.induction_loop_id,
            dbSchema.Tables.induction_loop.loop_interval,
            updateInterval.seconds,
            setting.dbSchema.AggregateData.getTimeStampLabel(),
            _GLOBALS.zeroIndexTime,
            setting.dbSchema.AggregateData.getTimeStampLabel(),
            correctStart,
            DETECTOR_FILTER))
    for row in rows:
        _GLOBALS.origData.add(tuple(row))


@benchmark
def identify_errors(rows, checkDoubling, hasLkw):
    """Check for typical data errors and remove erroneous values. This creates
    gaps. Return the number of errors found for each attribute."""
    error_counts = defaultdict(int)
    ignore = defaultdict(list)
    for row in rows:
        id, det, data_time, qKFZ, qLKW, vPKW, vLKW, detectorType = row
        if checkDoubling:
            entry = tuple(row[1:]) # see storeOrigData
            if entry in _GLOBALS.origData:
                continue
            _GLOBALS.origData.add(entry)
        date = roundToMinute(as_time(data_time), _GLOBALS.updateInterval)

        fixedDateIndex = fixDate(_GLOBALS.data[det], date, ignore[det])
        # fixedDateIndex may be an invalid index if the fix exceeds the length of the data list
        if fixedDateIndex >= len(_GLOBALS.data[det]):
            continue
        if date in ignore[det]:
            continue
        if detectorType == "highway": # hack for errors on highway detectors
            if qKFZ == 255:
                qKFZ = None
            if qLKW == 255:
                qLKW = None
        data = Data(id, det, date, qKFZ, qLKW, vPKW, vLKW)
        data.check(_GLOBALS.updateInterval, hasLkw)
        try:
            _GLOBALS.data[det][fixedDateIndex] = data
        except:
            print("ERROR: could not insert detector %s at index %s (%s, %s)" % (
                    det, fixedDateIndex, len(_GLOBALS.data[det]), date))
            sys.exit()
        # check for error 4
        data.set_hanging(is_hanging(_GLOBALS.data[det], fixedDateIndex))
        # update error counts
        for a in Data.attrs:
            if getattr(data, a) is None:
                error_counts[a] += 1
    return error_counts


@benchmark
def fixGaps(fixStart, fixEnd, forecast):
    """Wrapper for fixing gaps: prepares data lists, call polynomialFix and
    collect fixed data. Returns the fixed data and the number of fixes for each
    attribute"""
    if forecast:
        emptyID = Data.FORECAST_DATA
    else:
        emptyID = Data.NO_ORIG_DATA
    fix_counts = defaultdict(lambda:0)
    fixStartIndex = dateToIndex(fixStart)
    fixEndIndex = dateToIndex(fixEnd)
    for det, detData in _GLOBALS.data.items():
        # call unfix between fixStart and fixEnd. This avoids interpolated
        # values being used as support and also sets toBeWritten=True
        for index in range(fixStartIndex, fixEndIndex):
            if detData[index] is None:
                detData[index] = emptyData(det, emptyID)
            else:
                detData[index].unfix()

        #print "fixing detector %s" % det
        polynomialFix(detData, fixStartIndex, fixEndIndex, fix_counts, forecast)
    return fix_counts


@benchmark
def write_corrected(conn, correctStart, hasLkw):
    # collect data to be written
    fixedData = []
    # comment it out for Leipzip
    if not getOptionBool("Database", "postgres"):
        deleteData = []
    for detData in _GLOBALS.data.values():
        for index, time, data in _GLOBALS.enumerate_with_time(detData, dateToIndex(correctStart)):
            values = data.toValues(time, hasLkw)
            if values is not None:
                fixedData.append(values)
            data.toBeWritten = False
            # comment it out for Leipzip
            if not getOptionBool("Database", "postgres") and values is not None and values is not None:
                deleteData.append("(TIMESTAMP '%s', '%s')" % (time, data.detID))

    if len(fixedData) > 0:
        # Note: building one big query is much faster than using executemany
        if getOptionBool("Database", "postgres"):
            command = ("""INSERT INTO %s(%s) VALUES """ % (
                              dbSchema.Tables.corrected_loop_data,
                              dbSchema.CorrectDetector.corrected_columns) +
                          ','.join(fixedData) +
                      """ ON CONFLICT (data_time, det_id) DO UPDATE SET q_all= excluded.q_all, q_pkw = excluded.q_pkw, q_lkw = excluded.q_lkw, v_all = excluded.v_all, v_pkw = excluded.v_pkw, v_lkw = excluded.v_lkw, quality = excluded.quality""")

        else:
            command = "DELETE FROM %s WHERE (%s, %s) IN (%s)" % (
                        dbSchema.Tables.corrected_loop_data,
                        "DATA_TIME", "SENSOR_ID",
                        ','.join(deleteData))
            database.execSQL(conn, command, doCommit=True)
            prefix = " INTO %s(%s) VALUES " % (
                              dbSchema.Tables.corrected_loop_data,
                              dbSchema.CorrectDetector.corrected_columns)
            while len(fixedData) > 1000:
                command = "INSERT ALL " + prefix + prefix.join(fixedData[:1000]) + " SELECT 1 FROM DUAL"
                database.execSQL(conn, command, doCommit=True)
                del fixedData[:1000]
            command = "INSERT ALL " + prefix + prefix.join(fixedData) + " SELECT 1 FROM DUAL"
        database.execSQL(conn, command, doCommit=True)
    return len(fixedData)


def correction_summary(numDatapoints, error_counts, corrected_counts, 
        forecast_needed, forecast_counts, numWritten):
    summary = "db-lines read: %s, written %s" % (numDatapoints, numWritten)
    header = '\t'.join('attr errors corrected forecast_needed, forecast'.split())
    entries = ['\t'.join(map(str, [a, error_counts[a], corrected_counts[a],
        forecast_needed, forecast_counts[a]])) for a in Data.attrs]
    return '\n'.join([summary, header] + entries)


@benchmark
def correctDetector(isFirst, correctStart, correctEnd, forecastEnd,
                    interpolationWindow, evaluationInterval, updateInterval,
                    evalQuality=False):
    """
    Correct detector values by checking for obvious errors and
    interpolating (resp. extrapolating) missing values.
    """ 
    # There is a minor complication regarding the handling of times:
    # For historic reasons all time<->index conversions assume that the startTime
    # is inclusive and the endTime exclusive. However, since time-stamps in
    # the DB apply to data collected in the past, corrections up to a
    # certain time require all data up to that time (inclusive endTime)
    # To solve this problem we simply offset all input time stamps by one updateInterval
    correctStart += updateInterval
    correctEnd += updateInterval
    forecastEnd += updateInterval

    # this parameter is currently not needed since it depends on MAX_GAP_TIME
    interpolationWindow = MAX_GAP_TIME * 2
    # set detector type
    checkDoubling = getDetectorOptionBool("checkdoubling") 
    hasLkw = getDetectorOptionBool("haslkw")
    # create db connection
    conn = database.createDatabaseConnection()    
    # set _GLOBALS to the correct instance
    global _GLOBALS
    _GLOBALS = DATA_WINDOWS[updateInterval]
    if isFirst or len(_GLOBALS.data) == 0:
        _GLOBALS.reset(conn, updateInterval)
        # XXX this probably does not work
        #if checkDoubling:
        #     storeOrigData(conn, correctStart, updateInterval)
    # this requires _GLOBALS to be reset once before
    if evalQuality and _GLOBALS.new_quality_evaluation(correctStart, evaluationInterval):
        evalDetectorQuality(conn, correctStart - evaluationInterval, correctStart, updateInterval)
    # prepare
    # XXX roundToMinute does not do the right thing if updateInterval is not an integer number of minutes
    newZeroTime = roundToMinute(correctStart - interpolationWindow, updateInterval, ROUND_DOWN)
    _GLOBALS.prepare_dataLists(newZeroTime, forecastEnd)
    if isFirst:
        load_previous_corrections(conn, newZeroTime, correctStart, updateInterval)  # changes needed for tests
    # get new raw data
    rows = dbSchema.CorrectDetector.get_measurements_for_interval(conn, correctStart, correctEnd, updateInterval, DETECTOR_FILTER)  #  changes needed for tests
    if not rows and getDetectorOptionBool("historic"):
        if not _GLOBALS.has_more_data_after(conn, correctEnd):
            conn.close()
            return False
    # we got raw data. lets start
    error_counts = identify_errors(rows, checkDoubling, hasLkw)
    # fix gaps
    corrected_counts = fixGaps(correctStart, correctEnd, forecast=False)
    forecast_counts = fixGaps(correctEnd, forecastEnd, forecast=True)
    # update db
    num_written = write_corrected(conn, correctStart, hasLkw)
    conn.close()
    # reporting
    forecast_needed = (dateToIndex(forecastEnd) - dateToIndex(correctEnd)) * len(_GLOBALS.data)
    print(correction_summary(len(rows), error_counts, corrected_counts,
            forecast_needed, forecast_counts, num_written))
    print("end correct errors : %s TEXTTEST_IGNORE" % datetime.now())
    return True


def main(isFirst, beginNewDay, loopDir, options): 
    """
    Main method called from the loop. Calls successively correctDetector, 
    aggregateDetector, aggregateFCD and fusion. 
    """ 
    # init setting.edges
    if not setting.edges:
        region = getLoopOption("region")
        root = os.path.abspath(os.path.join(loopDir, region))
        edgePklFile = os.path.join(root, "infra", "edges.pkl")
        if os.path.isfile(edgePklFile):
            edgePkl = open(edgePklFile, 'rb')
            setting.edges = pickle.load(edgePkl)
            edgePkl.close()
        else:
            setting.edges = []
    # init updateIntervals
    if setting.updateIntervals is None:
        conn = database.createDatabaseConnection()    
        query = "SELECT DISTINCT (%s) from %s " % (
                dbSchema.Tables.induction_loop.loop_interval,
                dbSchema.Tables.induction_loop)
        available_intervals = [row[0] for row in database.execSQL(conn, query)]
        conn.close()
        if setting.hasOption("Detector", "updateinterval"):
            setting.updateIntervals = []
            try:
                # value should be a comma-separated list of minutes as float
                for minutes in setting.getDetectorOption("updateinterval").split(','):
                    seconds = float(minutes) * 60
                    if seconds in available_intervals:
                        setting.updateIntervals.append(seconds)
                    else:
                        print(("Warning: updateinterval %s does not exist in the database" % minutes))
            except:
                print(("Error:Could not parse value of Detector option 'updateinterval = %s' as a list of floats" % value))
                return False
        else:
            setting.updateIntervals = available_intervals
        if len(setting.updateIntervals) == 0:
            print("Warning: Empty list of detector update intervals: skipping correction and aggregation of induction loop data")
        else:
            setting.updateIntervals = [timedelta(seconds=s) for s in setting.updateIntervals]

    isFirst = isFirst or beginNewDay or setting.errorOnLastRun
    # set times
    if isFirst:
        correctStart = setting.startTime - getDetectorOptionMinutes("firstlookback")
    else:
        correctStart = setting.startTime - getDetectorOptionMinutes("lookback")    
    correctEnd = setting.startTime + getDetectorOptionMinutes("lookahead")
    if getDetectorOptionBool("doForecast"):
        forecastEnd = correctEnd + getLoopOptionMinutes("forecast")
    else:
        forecastEnd = correctEnd
    loopRawForecastEnd = correctEnd # forecasting takes place at the aggregated level
    aggregate = getLoopOptionMinutes("aggregate")
    aggStart = roundToMinute(correctStart, aggregate, ROUND_DOWN)
    aggEnd = roundToMinute(loopRawForecastEnd, aggregate, ROUND_UP)
    assert(aggregate > timedelta(0)) # otherwise aggregation will not terminate
    beginTime = datetime.now()    
    print("""\
-----------------------------------------------------------------------------
Data correction and aggregation from %s to %s
 with data forecast up to %s,
 starting at %s. TEXTTEST_IGNORE
- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\
""" % (correctStart, correctEnd, forecastEnd, beginTime))
    # STEPS
    setting.step = 1
    setting.errorOnLastRun = False
    # 1. Correcting detector data
    #if options.do_correction and setting.updateIntervals:
    if getDetectorOptionBool("doDetectorCorrection") and setting.updateIntervals:
        for updateInterval in setting.updateIntervals:
            result = pythonStep("Correcting detector data with updateInterval %s" % updateInterval, correctDetector,
                    (isFirst, correctStart, correctEnd, loopRawForecastEnd,
                        getDetectorOptionMinutes("interpolationwindow"),
                        getDetectorOptionMinutes("evaluationinterval"), updateInterval))
    else:
        result = True
    # 2. Aggregating detector data
    if getDetectorOptionBool("doDetectorAggregation") and setting.updateIntervals:
        for updateInterval in setting.updateIntervals:
            pythonStep("Aggregating detector data with updateInterval %s" % updateInterval, aggregateData.aggregateDetector,
                    (aggStart, aggEnd, aggregate, updateInterval))
    ## 2-1. filling data gap with use of historic data
    #if getDetectorOptionBool("fillDataGap"):
    #    sourceType = 'loop' # ther is no 'fusion' yet
    #    pythonStep("Data extrapolation to fill data gaps", extrapolation.main,
    #               (correctStart, correctEnd, aggregate, sourceType, True))
    #
    # 3. Aggregating FCD
    aggregateFCD = getDetectorOptionMinutes("aggregateFCD")
    if aggregateFCD > timedelta(0):
        aggFCDStart = roundToMinute(correctStart, aggregateFCD, ROUND_DOWN)
        aggFCDEnd = roundToMinute(correctEnd, aggregateFCD, ROUND_UP)
        pythonStep("Aggregating FCD", aggregateData.aggregateFCD,
                   (aggFCDStart, aggFCDEnd, aggregate, aggregateFCD,
                    getDetectorOptionMinutes("tlsWaitFCD")))
    # 4. Correcting visual data
    if dbSchema.CorrectDetector.enableVisual:                           # yp: currently alwasy false....
        pythonStep("Correcting visual data", correctVisual.correctVisual,
                   (correctStart, correctEnd))
    # 5. Aggregating visual data
    if dbSchema.CorrectDetector.enableVisual:                           # yp: currently alwasy false....
        aggregateVisual = getDetectorOptionMinutes("aggregateVisual")
        if aggregateVisual > timedelta(0):
            aggVisualStart = roundToMinute(correctStart, aggregateVisual, ROUND_DOWN)
            aggVisualEnd = roundToMinute(correctEnd, aggregateVisual, ROUND_UP)
            pythonStep("Aggregating visual data", correctVisual.aggregateVisual,
                       (aggVisualStart, aggVisualEnd, aggregateVisual))
    # 6. Data fusion
    if getDetectorOptionBool("doFusion") and aggregate > timedelta(0):
        pythonStep("Data fusion", fusion.fusion,
                   (aggStart, aggEnd, aggregate))
    # 7. Data extrapolation
    if forecastEnd != correctEnd:
        sourceType = 'fusion' if getDetectorOptionBool("doFusion") else 'loop'
        pythonStep("Data extrapolation", extrapolation.main,
                   (correctEnd, forecastEnd, aggregate, sourceType))

    endTime = datetime.now()
    print("""\
Data correction and aggregation ended at %s. TEXTTEST_IGNORE
Duration: %s TEXTTEST_IGNORE
-----------------------------------------------------------------------------\
""" % (endTime, endTime - beginTime))
    return result != False
