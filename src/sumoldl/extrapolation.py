# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    extrapolation.py
# @author  Jakob Erdmann
# @author  Michael Behrisch
# @date    2012-07-27
"""
Helper functions for extrapolating traffic data
Used by correctDetector
"""

import os,sys
import math
from datetime import datetime, timedelta
from collections import defaultdict
import database
import setting
from setting import dbSchema
from detector import DetectorReader
from evalDetector import Data
from aggregateData import insertAggregated
from database import as_time
from tools import geh, SAFE_ADD, SAFE_SUB, getIntervalEndsBetween

IGNORE_REGION = True # extrapolate for all edges regardless of region
TIME_OFFSETS = [timedelta(days=7), timedelta(days=14), timedelta(days=21)]
SMOOTHING_WIDTH = 2 # symmetrical width of the smoothing range
SMOOTHING_RANGE = range(-SMOOTHING_WIDTH, SMOOTHING_WIDTH + 1) # offsets around to target date for averaging
VALIDATION_WIDTH = 3 # maximum number of datapoints to use for validating the extrapolation
FLOW, SPEED = range(2)
FEEDBACK_WIDTH = 2
FEEDBACK_RANGE = range(-FEEDBACK_WIDTH,1) # compute correction based on the primaryPrediction of the last 3 known measurements
# Debugging
DEBUG = False
EDGE_FILTER = ("" if not DEBUG else "and edge_id = 186390")

def safe_avg(values, default=None):
    actualValues = [v for v in values if v is not None]
    if actualValues:
        return sum(actualValues) / float(len(actualValues))
    else:
        return default

def smooth_predictor(offsets):
    """create primary predictive function which uses the weekly periodicity of traffic
       (see constants TIME_OFFSETS and SMOOTHING_RANGE)"""
    def predictor(timeValues, time):
        maybeValues = [timeValues.get(time - o) for o in offsets]
        flows = [v[FLOW] for v in maybeValues if v is not None]
        speeds = [v[SPEED] for v in maybeValues if v is not None]
        return (safe_avg(flows), safe_avg(speeds))
    return predictor


def get_correction(timeValues, knownTime, primaryPredictor):
    result = [0, 0]
    knownValue = timeValues.get(knownTime)
    if knownValue is not None:
        knownPred = primaryPredictor(timeValues, knownTime)
        result = map(SAFE_SUB, knownValue, knownPred)
    if DEBUG:
        print knownTime, 'correction:', result
    return result


def feedback_predictor_absolute(primaryPredictor, knownTime):
    """correct extrapolation with the absolute extrapolation error for a known measurement"""
    def predictor(timeValues, time):
        primary = primaryPredictor(timeValues, time)
        correction = get_correction(timeValues, knownTime, primaryPredictor)
        # make sure only valid data points are returned
        primaryData = Data(None, None, None, primary[FLOW], None, primary[SPEED], None)
        if primaryData.qPKW is not None and correction[FLOW] is not None:
            primaryData.fix('qPKW', primaryData.qPKW + correction[FLOW], timedelta(seconds=3600))
        if primaryData.vPKW is not None and correction[SPEED] is not None:
            primaryData.fix('vPKW', primaryData.vPKW + correction[SPEED], timedelta(seconds=3600))
        return (primaryData.qPKW, primaryData.vPKW)
    return predictor


def get_traffic_ids(conn, type, intervalEnds):
    result = {} # traffic_id -> time
    times = ','.join(["%s '%s'" %(dbSchema.AggregateData.getTimeStampLabel(), t) for t in intervalEnds])
    intervalTable = dbSchema.AggregateData.getSchema(type)[0]
    command = """select %s, %s from %s where %s in (%s)
                 %s
                """ % (dbSchema.Tables.loop_traffic.traffic_id,
                        dbSchema.Tables.traffic.traffic_time,
                        intervalTable,
                        dbSchema.Tables.traffic.traffic_time,
                        times, 
                        dbSchema.Extrapolation.getTypePredicate(type))
    #print 'command:\n', command
    rows =  database.execSQL(conn, command)
    for id, time in rows:
        result[int(id)] = as_time(time)
    return result


def get_data_for_traffic_ids(conn, type, ids):
    result = defaultdict(dict) # edge -> (time -> [q,v])
    if len(ids) == 0:
        return result
    regionFilter = None
    if not IGNORE_REGION:
        edgeMap = dbSchema.AggregateData.getSimulationEdgeMap(conn)
        regionFilter = set([edgeMap[nId] for nId in edgeMap.iterkeys() if nId in setting.edges])
        #regionFilterSql = 'and edge_id in (%s)' % ','.join(map(str,regionFilter))
    intervalTable, dataTable, q_column, v_column = dbSchema.AggregateData.getSchema(type)
    edge_id = "edge_id"
    if dbSchema.Loop.region_choices[0] == "huainan":
        edge_id = "fbd_id"
    command = """
                select %s, %s, %s, %s from %s
                where %s in (%s)
                %s
                """ % (dbSchema.Tables.traffic.traffic_id,
                        edge_id,
                        q_column, v_column,
                        dataTable,
                        dbSchema.Tables.traffic.traffic_id,
                        ','.join(map(str,ids.keys())), EDGE_FILTER)
    #print 'command:\n', command
    rows =  database.execSQL(conn, command)
    num_used_values = 0
    for id, edge, q, v in rows:
        if regionFilter is None or edge in regionFilter:
            num_used_values += 1
            if q is not None:
                q = float(q)
            if v is not None:
                v = float(v)
            result[edge][ids[id]] = (q,v)
    print 'Fetched %s values and kept %s for %s edges TEXTTEST_IGNORE' % (len(rows), num_used_values, len(result))
    return result


def predict_at_times(times, data, predictor):
    result = defaultdict(dict) # edge -> (time -> [q,v])
    for edge in data.iterkeys():
        for time in times:
            result[edge][time] = predictor(data[edge], time)
    return result

def estimate_quality(lastKnown, data, intervalLength, predictor):
    # times for which measurements are already known
    times = getIntervalEndsBetween(lastKnown - VALIDATION_WIDTH * intervalLength, lastKnown, intervalLength)
    #print "validating quality using measurements for times: %s" % times
    predData = predict_at_times(times, data, predictor)
    # compare
    flowScale = 3600. / intervalLength.seconds
    result = {}
    for edge in data.iterkeys():
        result[edge] = safe_avg(
                [pred_quality_at_time(edge, time, data, predData, flowScale) for time in times],
                default=-1) # allows numerical comparison when retrieving data
    return result

def geh_to_quality(g):
    """estimate quality based on GEH with a linear mapping:
       GEH 0 -> quality 100
       GEH 5 -> quality 50
       GEH >= 10 -> quality 0"""
    return max(0, 100 - 10*g) if g is not None else None


def pred_quality_at_time(edge, time, data, predData, flowScale):
    if not time in data[edge]:
        return None
    flow, speed = data[edge][time]
    predFlow, predSpeed = predData[edge][time]
    if flow is not None and predFlow is not None:
        return geh_to_quality(geh(flow * flowScale, predFlow * flowScale))
    elif speed is not None and predSpeed is not None:
        # compute pseudo-GEH by mapping speeds onto a range of plausible flows
        return geh_to_quality(geh(speed * 100, predSpeed * 100))
    else:
        return None


def main(start, end, intervalLength, sourceType):
    """Main entry point of this module. """
    conn = database.createDatabaseConnection()
    # feedbackInterval determines how much extra data must be loaded for the feedback predictor
    feedbackInterval = end - start
    offsets = set([o + i * intervalLength for o in TIME_OFFSETS for i in SMOOTHING_RANGE])
    predictor = feedback_predictor_absolute(smooth_predictor(offsets), start - intervalLength)
    # load historical data and latest measurements for validation
    loadTimes = getIntervalEndsBetween(
            start - feedbackInterval - intervalLength * VALIDATION_WIDTH, start, intervalLength)
    for offset in TIME_OFFSETS:
        loadTimes += getIntervalEndsBetween(
                start - offset - feedbackInterval - intervalLength * (SMOOTHING_WIDTH + VALIDATION_WIDTH), 
                end   - offset + intervalLength * SMOOTHING_WIDTH, 
                intervalLength)
    trafficIDs = get_traffic_ids(conn, sourceType, loadTimes)
    data = get_data_for_traffic_ids(conn, sourceType, trafficIDs)
    # extrapolate data
    times = getIntervalEndsBetween(start, end, intervalLength)
    predData = predict_at_times(times, data, predictor)
    # compute quality
    quality = estimate_quality(start, data, intervalLength, predictor)
    # write to DB
    detReader = DetectorReader()
    for edge in predData.iterkeys():
        detReader.addGroup(0, edge)
        detReader.addDetector(edge, 0, edge)
    qualitySum = 0
    writtenEntries = 0
    writtenEdges = set()
    for time in times:
        for edge in predData.iterkeys():
            flow, speed = predData[edge][time]
            if flow is not None or speed is not None:
                qual = quality[edge]
                detReader.addFlow(edge, flow, speed, qual)
                writtenEntries += 1
                writtenEdges.add(edge)
                qualitySum += max(qual, 0)
        #print "inserting values for time %s" % time
        insertAggregated(conn, "extrapolation", detReader, time, intervalLength)
    avgQuality = (qualitySum / writtenEntries if writtenEntries > 0 else None)
    print 'Extrapolated %s data points for %s edges with average quality %s TEXTTEST_IGNORE' % (
            writtenEntries, len(writtenEdges), avgQuality)
    conn.close()
