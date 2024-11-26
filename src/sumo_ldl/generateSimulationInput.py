# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    generateSimulationInput.py
# @author  Michael Behrisch
# @date    2007-08-02
"""
Functions for reading detector data from the database and generating
appropriate triggers for the simulation. Usually it is called from runStep.py.
"""
import os, sys, re
from datetime import datetime, timedelta
from collections import defaultdict

from . import setting, tools, database
from .setting import dbSchema
from .database import as_time
from .tools import reversedMap

class ListWrapper(list):
    """wrapper for normal python lists which limits the output when printing"""
    def __init__(self, l, toPrint=5):
        self.toPrint = toPrint
        super(ListWrapper, self).__init__(l)

    def __str__(self):
        dots = (', ...' if len(self) > self.toPrint else '')
        return "<list with %s items [%s%s]>" % (
                len(self), ','.join(map(str,self[:self.toPrint])), dots)

    def __repr__(self):
        return self.__str__()


def _writeCalibrators(filename, flowMap, routeInterval, begin, calibratorInterval, logfile, collectRouteInfo):
    """Writes the time dependent data to the individual files."""
    with open(filename, 'w') as f:
        print('<?xml version="1.0"?>\n<add>', file=f)
        for edge, flowsteps in flowMap.items():
            routeProbe = (' routeProbe="routedist_%s"' % edge) if collectRouteInfo else ""
            print('    <calibrator id="calibrator_%s" lane="%s_0" pos="0" freq="%s" friendlyPos="x" output="%s"%s>' % (
                edge, edge, calibratorInterval, logfile, routeProbe), file=f)
            for time, aggInterval, flow, speed, quality, type in sorted(flowsteps):
                if speed is None or speed > 120.:  # todo: set a filter if speed is very low especially at late night and if speed is very high > 100 except of highway
                    speed_attr = '' # disable speed calibration if speed is not known (see METriggeredCalibrator::execute())
                else:
                    speed_attr = 'speed="%s" ' % speed
                flow_attr = '' if flow is None else 'vehsPerHour="%s" ' % flow # disable flow calibration if flow is not known
                startSecond = tools.daySecond(time - timedelta(seconds=aggInterval), begin)
                comment, forceMultiplier = (('comment="extrapolation"', 0.5) if type == 'extrapolation' else ('', 1.0))
                force = quality * forceMultiplier
                # calibrator prefers the dynamic route distribution (with interval time as suffix) 
                # and uses the static route distribution as fallback
                flowElement = '        <flow begin="%s" end="%s" %s%svType="vtypedist" route="routedist_%s" force="%s" %s/>' % (
                        startSecond, startSecond + aggInterval, flow_attr, speed_attr, edge, force, comment)
                print(flowElement, file=f)
            print("    </calibrator>", file=f)
        print("</add>", file=f)


def generateCalibrators(directory, simBegin, forecastStart, simEnd, simOutputDir):
    """Main function of this module. Parses the detector file,
    reads the DB and calls the other functions."""
    routeInterval = setting.getLoopOptionMinutes("routeInterval")
    doSpeedCalibration = setting.getLoopOptionBool("speedCalibration")
    types = [setting.getLoopOption("calibrationSource")]
    if setting.getDetectorOptionBool("doForecast"):
        types += ['extrapolation']
    # when running in historic mode, ignore measurements from the future
    discardFutureMeasurements = setting.getDetectorOptionBool("historic")
    # query DB for all types but use only the first row for each (time,edge)
    # (extrapolation comes last in sort order)
    trafficData = defaultdict(lambda:[]) # navteqID -> [(time, interval, flow, speed, quality, type), ...]
    conn = database.createDatabaseConnection()
    rows = dbSchema.GenerateSimulationInput.getTypedTrafficValues(conn, types, simBegin, simEnd, 
            setting.getLoopOption("qualityThreshold"),
            setting.getLoopOptionMinutes("aggregate").seconds,
            setting.timeline)
    typeCounts = defaultdict(lambda:0)
    covered_times = defaultdict(set)
    flowEdges = set()
    for id, time, interval, flow, speed, quality, type in rows:
        if not id in setting.edges:
            continue
        time = database.as_time(time)
        if flow is None and speed is None:
            print("Warning: ignoring invalid entry %s" % ((id, time, interval, flow, speed, quality, type),), file=sys.stderr)
            continue
        time = as_time(time)
        if discardFutureMeasurements and type != 'extrapolation' and time > forecastStart:
            continue
        if flow is None and not doSpeedCalibration:
            continue
        if time in covered_times[id]:
            continue # real measurement is known, do not used extrapolation
        if flow is not None:
            flowEdges.add(id)
        covered_times[id].add(time)
        typeCounts[type] += 1
        trafficData[id].append((time, interval, flow, speed, quality, type))
    print("Fetched %s entries for %s edges types=%s TEXTTEST_IGNORE" % (len(rows), len(trafficData), dict(typeCounts)))
    conn.close()
    # write calibrators
    calibratorAdd = os.path.join(directory, "calibrators.add.xml")
    _writeCalibrators(calibratorAdd, 
            trafficData, routeInterval,
            tools.daySecond(simBegin), setting.getLoopOption("calibratorInterval"),
            os.path.join(simOutputDir, "calibrators.log.xml"), setting.getLoopOptionBool('collectRouteInfo')),
    return [calibratorAdd], ListWrapper(flowEdges)

def calculateInterval(begin, end, navteqTime):
    """
    Calculate the intersection of the time interval(s) given in the navteq description
    with the interval described by (begin, end). Returns the interval as pair or
    None on empty intersection or navteq parsing failure.
    """
    match = re.match('\[\((\w*)\)\{(\w*)\}\]', navteqTime)
    if not match or len(match.groups()) < 2:
        print("Warning! Unsupported NavTeq time format %s." % navteqTime, file=sys.stderr)
        return None
    navteqBegin = match.group(1)
    parsedBegin = begin + timedelta(0)
    for datepart in ["y", "M", "d", "h", "m", "s"]: 
        if datepart == navteqBegin[0]:
            idx = 1
            while idx < len(navteqBegin) and navteqBegin[idx] in "0123456789":
                idx += 1
            amount = int(navteqBegin[1:idx])
            if idx < len(navteqBegin):
                navteqBegin = navteqBegin[idx:]
            if datepart == "y":
                parsedBegin = parsedBegin.replace(amount) 
            elif datepart == "M":
                maxDay = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                if parsedBegin.year % 4 == 0 and parsedBegin.year % 100 != 0:
                    maxDay[2] = 29
                targetDay = min(parsedBegin.day, maxDay[amount])
                parsedBegin = parsedBegin.replace(month=amount, day=targetDay) 
            elif datepart == "d":
                parsedBegin = parsedBegin.replace(day=amount) 
            elif datepart == "h":
                parsedBegin = parsedBegin.replace(hour=amount) 
            elif datepart == "m":
                parsedBegin = parsedBegin.replace(minute=amount) 
            elif datepart == "s":
                parsedBegin = parsedBegin.replace(second=amount) 
    navteqInterval = match.group(2)
    parsedEnd = parsedBegin + timedelta(0)
    for datepart in ["y", "M", "w", "d", "h", "m", "s"]: 
        if datepart == navteqInterval[0]:
            idx = 1
            while idx < len(navteqInterval) and navteqInterval[idx] in "0123456789":
                idx += 1
            amount = int(navteqInterval[1:idx])
            if idx < len(navteqInterval):
                navteqInterval = navteqInterval[idx:]
            if datepart == "y":
                parsedEnd = parsedEnd.replace(parsedEnd.year + amount) 
            elif datepart == "M":
                years = amount / 12
                targetMonth = parsedEnd.month + amount % 12
                if targetMonth > 12:
                    targetMonth -= 12
                    years += 1
                parsedEnd = parsedEnd.replace(parsedEnd.year + years) 
                if targetMonth != parsedEnd.month:
                    maxDay = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                    if parsedEnd.year % 4 == 0 and parsedEnd.year % 100 != 0:
                        maxDay[2] = 29
                    targetDay = min(parsedEnd.day, maxDay[targetMonth])
                    parsedEnd = parsedEnd.replace(month=targetMonth, day=targetDay) 
            elif datepart == "w":
                parsedEnd += timedelta(weeks=amount) 
            elif datepart == "d":
                parsedEnd += timedelta(days=amount)
            elif datepart == "h":
                parsedEnd += timedelta(hours=amount)
            elif datepart == "m":
                parsedEnd += timedelta(minutes=amount) 
            elif datepart == "s":
                parsedEnd += timedelta(seconds=amount)
    if parsedEnd < parsedBegin:
        parsedEnd, parsedBegin = parsedBegin, parsedEnd 
    if parsedEnd < begin or parsedBegin > end:
        return None 
    if parsedBegin > begin:
        begin = parsedBegin
    if parsedEnd < end:
        end = parsedEnd 
    return begin, end

def handleBlockings(directory, intervalBegin, intervalEnd, testBlockingRows = []):
    """Second main function of this module. Reads the DB
    for blockings and generates the input files."""
    conn = database.createDatabaseConnection()
 
    if len(testBlockingRows) == 0:
        rows = database.execSQL(conn,
                                dbSchema.GenerateSimulationInput.getRestrictionQuery(intervalBegin))
    else:
        rows = testBlockingRows

    blockedSections = {}
    if len(rows) == 0:
        return []

    reverseEdgeMap = reversedMap(dbSchema.AggregateData.getSimulationEdgeMap(conn)) # for 1-to-1 edge relation, not 1-to-more edge relations
    numRerouters = 0
    additional = os.path.join(directory, "blockings.add.xml")
    with open(additional, 'w') as f:
        print("<add>", file=f)
        # add vaporizers on blocked edges
        for edge_id, validity_period in rows:
            interval = calculateInterval(intervalBegin, intervalEnd, validity_period)
            if interval:
                edge_id_sim = reverseEdgeMap.get(edge_id, edge_id) # for 1-to-1 edge relation
                begin, end = interval
                beginSecond = tools.daySecond(begin)
                endSecond = tools.daySecond(end, beginSecond)
                print('    <vaporizer id="%s" begin="%s" end="%s"/>'\
                            % (edge_id_sim, beginSecond, endSecond), file=f)
                blockedSections[edge_id] = (beginSecond, endSecond, edge_id_sim)

        # put rerouters on edges leading to blocked sections
        for edge_id, blocking in list(blockedSections.items()):
            rows = database.execSQL(conn, 
                    """ SELECT %s FROM %s WHERE %s=%s""" % (
                        dbSchema.Tables.edge_connection.in_edge,
                        dbSchema.Tables.edge_connection,
                        dbSchema.Tables.edge_connection.out_edge,
                        edge_id))

            rerouter_edges = [reverseEdgeMap.get(e[0],e[0]) for e in rows if not e[0] in blockedSections] # for 1-to-1 edge relation
            if len(rerouter_edges) > 0:
                numRerouters += 1
                edge_id_sim = reverseEdgeMap.get(edge_id, edge_id) # for 1-to-1 edge relation
                print("""    <rerouter id="rerouter_%s" edges="%s">
        <interval begin="%s" end="%s">
            <closingReroute id="%s"/>
        </interval>
    </rerouter>""" % ((edge_id_sim, ' '.join(rerouter_edges)) + blocking), file=f)

        print("</add>", file=f)
    print("Blocked %s edges and placed %s rerouters." % (len(blockedSections), numRerouters))
    return [additional]
