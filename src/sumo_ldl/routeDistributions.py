#!/usr/bin/env python
# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    routeDistributions.py
# @author  Michael Behrisch
# @date    2009-01-10
"""
Generates static and dynamic route distributions for certain
edges in the network. Usually called by simulationRun.py.
"""
import os, shutil
from datetime import datetime, timedelta
import collections

from . import setting, tools, database
from .setting import dbSchema
from .tools import reversedMap

STATIC = set()
DYNAMIC = set()
INVALID = set()
LAST_RESET = None


def checkReset(isFirst, time):
    """reset once per day but try to do it in the morning hours. Return whether
    a reset took place"""
    global LAST_RESET
    if isFirst or (
            LAST_RESET and 
            time - LAST_RESET >= timedelta(days=1) and
            time.hour < 4):
        DYNAMIC.clear()
        INVALID.clear()
        LAST_RESET = time
        return True
    else:
        return False


def generateDynamic(file, isFirst, intervalBegin, intervalEnd, routeInterval):
    qualityThreshold = int(setting.getLoopOption("qualityThreshold"))
    typeName = setting.getLoopOption("calibrationSource")
    reset = checkReset(isFirst, intervalBegin)
    conn = database.createDatabaseConnection()
    intervalTable, dataTable, q_column, v_column = dbSchema.AggregateData.getSchema(typeName)
    command = """SELECT edge_id, a.quality
                 FROM %s t, %s a
                 WHERE a.%s = t.%s AND %s > '%s' AND %s <= '%s' AND %s IS NOT NULL %s
        """ % (intervalTable, dataTable,
            dbSchema.Tables.traffic.traffic_id,
            dbSchema.Tables.traffic.traffic_id,
            dbSchema.Tables.traffic.traffic_time, intervalBegin,
            dbSchema.Tables.traffic.traffic_time, intervalEnd,
            q_column,
            dbSchema.Extrapolation.getTypePredicate(typeName))
    rows = database.execSQL(conn, command)

    reverseEdgeMap = reversedMap(dbSchema.AggregateData.getSimulationEdgeMap(conn))
    for edge_id, quality in rows:
        edge_id = reverseEdgeMap.get(edge_id, edge_id)
        if edge_id in setting.edges:
            if reset and int(quality) < qualityThreshold:
                INVALID.add(edge_id)
            elif edge_id not in INVALID:
                DYNAMIC.add(edge_id)
    routeStart = tools.daySecond(tools.roundToMinute(intervalEnd - routeInterval, routeInterval, tools.ROUND_DOWN)) 
    with open(file, 'w') as f:
        print('<?xml version="1.0"?>\n<add>', file=f)
        for edge in DYNAMIC:
            print('    <routeProbe id="routedist_%s" edge="%s"' % (edge, edge), end=' ', file=f)
            print('freq="%s" begin="%s" file="NUL"/>' % (routeInterval.seconds, routeStart), file=f) 
        print('</add>', file=f)


def generateStatic(file, isFirst, intervalBegin, intervalEnd, edges, routeDir):
    """copy pre-generated static route distribution for all new edges from the
    given list of edges"""
    if isFirst:
        STATIC.clear()
    #newEdges = set(edges).difference(STATIC)
    #STATIC.update(newEdges)

    # get the mapping between FBD_ID and SUMO_ID
    conn = database.createDatabaseConnection()
    edgeMap = dbSchema.AggregateData.getSimulationEdgeMap(conn, True)  # sumo_id = [fbd_id...]
    uncoveredFbd = set()
    coveredFbd = set()

    # use route distribtuion from a existing edge, when more than one sumo edges are mapped to a fbd_id and one or more of the sumo edges have detectors.
    matchedMap = collections.defaultdict(list)
    missedEdges = set()

    # all edges will be generated, not only the new coming ones
    with open(file, 'w') as distributions:
        print('<?xml version="1.0"?>\n<routes>', file=distributions)
        for edge in edges:
            dirName = ""
            if len(edge) > 2:
                dirName = edge[:2]
                if edge[0] == "-":
                    dirName = edge[1:3]
                if os.path.exists(os.path.join(routeDir,dirName,edge)):
                    f = open(os.path.join(routeDir,dirName,edge))
                    shutil.copyfileobj(f, distributions)
                    f.close()
                    coveredFbd.update(edgeMap[edge])
                    for fid in edgeMap[edge]:
                        matchedMap[fid].append(edge)
                else:
                    uncoveredFbd.update(edgeMap[edge])
                    missedEdges.add(edge)
        for me in missedEdges:
            # get the reference edge
            findRefEdge = True
            for fid in edgeMap[me]:
                if matchedMap[fid]:
                    edge = matchedMap[fid][0] # use the route distribution of the first edge
                    if len(edge) > 2 and findRefEdge:
                        dirName = edge[:2]
                        if edge[0] == "-":
                            dirName = edge[1:3]
                        if os.path.exists(os.path.join(routeDir,dirName,edge)):
                            f = open(os.path.join(routeDir,dirName,edge))
                            for line in f:
                                elems = line.split('"')
                                print('%s"routedist_%s"%s"%s"%s"%s"%s\n' %(elems[0],me,elems[2],elems[3],elems[4],elems[5],elems[6]), file=distributions)
                            f.close()
                        findRefEdge = False
            if findRefEdge:
                print('Warning: no existing route distribtuion file sutiable for', me)
        print('</routes>', file=distributions)
    # check if any fbd_id that has detectors has no corresponding edge file containing route distribution
    uncoveredFbd.difference_update(coveredFbd)
    if uncoveredFbd:
        print('Warning: the file for edges', uncoveredFbd, 'does not exist.')
