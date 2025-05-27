# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    aggregateData.py
# @author  Michael Behrisch
# @date    2013-04-18
"""
Helper functions for data aggregation and database insertion.
Used by correctDetector, fusion and generateViewerInput.
"""
import sys, operator
from datetime import datetime, timedelta
from collections import defaultdict

from . import setting, database
from .detector import DetectorReader, MAX
from .database import as_time
from .tools import reversedMap

def insertAggregated(conn, typeName, detReader, intervalEnd, intervalLength, isSimulation=False, 
                     doClose=False, flowScale=1.0, expectedEntryCount=0):
    """Insert aggregated data into the database. The data is read from the
       given DetectorReader. If it is simulation data the edge ids are
       taken as Navteq IDs instead of database road_section IDs and scenarios are taken into account."""
    AggregateData = setting.dbSchema.AggregateData
    if conn == None:
        conn = database.createOutputConnection()
        doClose = True
    trafficIndex = None
    values = {}
    intervalTable, dataTable, q_column, v_column = AggregateData.getSchema(typeName)
    edgeMap = AggregateData.getSimulationEdgeMap(conn, True) if isSimulation else None
    # handle existing entry in traffic for the same type and time
    if isSimulation and setting.scenarioID:
        trafficIndex = AggregateData.getIntervalID(conn, typeName, intervalEnd, intervalLength, setting.scenarioID)
    else:
        trafficIndex = AggregateData.getIntervalID(conn, typeName, intervalEnd, intervalLength)
    # process the detReader
    totalQuality = 0.
    unknownEdges = 0
    for dataEdge, groups in detReader.getEdgeDataIterator():
        if isSimulation:
            if dataEdge not in edgeMap:
                unknownEdges += 1
                if unknownEdges < 10:
                    sys.stderr.write("Ignoring data for unknown simulation edge '%s'\n" % (dataEdge))
                continue
            else:
                edges = edgeMap[dataEdge] # make edge a database-id
        else:
            edges = [dataEdge]
        for edge in edges:
            flowSum, speedSum, qualitySum, coverageSum, entryCount, groupCount = values.get(edge, (None, None, None, None, 0, 0))
            # sum values for all groups
            for group in groups:
                if group.totalFlow is not None:
                    if flowSum is None:
                        flowSum = 0.0 # init flow on first usable data point
                    flowSum += group.totalFlow
                if group.avgSpeed is not None:
                    if speedSum is None:
                        speedSum = 0.0 # init speed on first usable data point
                    speedSum += group.avgSpeed * (group.totalFlow if group.totalFlow else 1)
                if group.quality is not None:
                    if qualitySum is None:
                        qualitySum = 0.0 # init quality on first usable data point
                    qualitySum += group.quality * group.entryCount
                if group.coverage is not None:
                    if coverageSum is None:
                        coverageSum = 0.0 # init coverage on first usable data point
                    coverageSum += group.coverage / len(group.detectors)
                entryCount += group.entryCount
                group.reset()
            groupCount += len(groups)
            values[edge] = (flowSum, speedSum, qualitySum, coverageSum, entryCount, groupCount)
    insertRows = []
    for edge, (flowSum, speedSum, qualitySum, coverageSum, entryCount, groupCount) in list(values.items()):
        if entryCount > 0:
        #    if edge == 7047:
        #        print('BEFORE INSERT', flowSum, speedSum, qualitySum, coverageSum, entryCount)
            if typeName == 'fcd':
                coverage = None if coverageSum is None else coverageSum / entryCount
            else:
                coverage = None if coverageSum is None else coverageSum / expectedEntryCount
            if flowSum is None:
                flow = None
            else:
                # scale flow according to coverage
                # if we only receive data for 3 minutes out of 5 we need to scale up.
                # in case aggregation is not a multiple of the data intervals we
                # may even have coverage >1 and need to scale down 
                if coverage is not None:
                    flow = flowSum / coverage
                else:
                    flow = flowSum
                # scale to veh/hour
                if isSimulation:
                    flow = int(flow * flowScale / entryCount)
                else:
                    flow = int(flow * flowScale / groupCount)
            if speedSum is None:
                speed = None
            else:
                speed = speedSum / (flowSum if flowSum else entryCount)
            if qualitySum is None:
                quality = None
            else:
                # if coverage differs from 1 quality is reduced
                if coverage is None:
                    coverageDiscount = 1.0
                elif coverage > 1:
                    coverageDiscount = 1.0 / coverage
                else:
                    coverageDiscount = coverage
                quality = qualitySum * coverageDiscount / entryCount
                totalQuality += quality
        #    if edge == 7047:
        #        print('AFTER INSERT', trafficIndex, edge, flow, speed, quality)
            insertRows.append((trafficIndex, edge, flow, speed, quality))
    # perform DB write
    if unknownEdges > 0:
        sys.stderr.write("Ignored data for %s unknown simulation edges'\n" % unknownEdges)
    if len(insertRows) > 0:
        # update quality; XXX quality of remaining old entries is ignored
        if coverageSum is not None:
            print(("Updating %s edges in %s with average coverage of %.2f TEXTTEST_IGNORE" % (
                  len(insertRows),
                  AggregateData.update_description(dataTable, typeName),
                  coverageSum / len(insertRows))))
        totalQuality /= len(insertRows)
        updateQualityQuery = "UPDATE %s SET quality = %s WHERE %s = %s" % (
                intervalTable,
                totalQuality,
                intervalTable.traffic_id,
                trafficIndex)
        database.execSQL(conn, [updateQualityQuery], True)
        AggregateData.insertData(conn, typeName, insertRows)
    if doClose:
        conn.close()


def aggregateDetector(start, end, intervalLength, updateInterval):
    """Time aggregation of detector data in the given interval in subintervals
       of the given length."""
    Tables = setting.dbSchema.Tables
    conn = database.createDatabaseConnection()
    detReader = DetectorReader()
    # init groups
    if setting.dbSchema.Loop.region_choices[0] in ("huainan", "leipzig"):
        rows = database.execSQL(conn, """
            SELECT i.%s, i.%s, e.%s, e.%s
            FROM %s i, %s g, %s e
            WHERE i.%s = g.%s
            AND g.%s = e.%s
            ORDER BY i.%s""" % (
                Tables.induction_loop.induction_loop_id,
                Tables.induction_loop.induction_loop_group_id,
                Tables.induction_loop_group_edge.edge_id,
                Tables.induction_loop_group_edge.road_km,
                Tables.induction_loop,
                Tables.induction_loop_group,
                Tables.induction_loop_group_edge,
                Tables.induction_loop.induction_loop_group_id,
                Tables.induction_loop_group.induction_loop_group_id,
                Tables.induction_loop_group.induction_loop_group_id,
                Tables.induction_loop_group_edge.induction_loop_group_id,
                Tables.induction_loop.induction_loop_group_id))
    else:
        rows = database.execSQL(conn, """
            SELECT i.induction_loop_id, i.induction_loop_group_id, g.edge_id, g.road_km
            FROM %s i, %s g
            WHERE i.induction_loop_group_id = g.induction_loop_group_id 
            ORDER BY i.induction_loop_group_id""" % (
                Tables.induction_loop,
                Tables.induction_loop_group))
    currGroup = None
    for det, group, edge, pos in rows:
        if group != currGroup:
            currGroup = group
            detReader.addGroup(pos, edge)
        detReader.addDetector(det, pos, edge)
    # get operating status
    #rows = database.execSQL(conn, """
    #    SELECT i.induction_loop_id FROM %s i, %s o
    #    WHERE i.induction_loop_group_id = o.induction_loop_group_id AND o.quality > 0
    #    AND (o.status_time , o.induction_loop_group_id) IN 
    #    (SELECT MAX(status_time), induction_loop_group_id FROM %s 
    #    WHERE status_time > '%s' AND status_time <= '%s' GROUP BY induction_loop_group_id)
    #    """ % (
    #        Tables.induction_loop,
    #        Tables.operating_status,
    #        Tables.operating_status,
    #        end-timedelta(2), end))
    #validDetectors = set()
    #for row in rows:
    #    validDetectors.add(row[0])
    # get corrected loop data
    rows = database.execSQL(conn, """
        SELECT c.%s, c.q_pkw, c.q_lkw, c.v_pkw, c.v_lkw, c.data_time, c.quality 
        FROM %s c, %s i WHERE c.%s = i.%s
        AND i.%s = %s
        AND data_time > %s '%s' 
        AND data_time <= %s '%s' 
        AND quality > 0
        ORDER BY data_time, %s""" % (
            Tables.corrected_loop_data.induction_loop_id,
            Tables.corrected_loop_data,
            Tables.induction_loop,
            Tables.corrected_loop_data.induction_loop_id,
            Tables.induction_loop.induction_loop_id,
            Tables.induction_loop.loop_interval,
            updateInterval.seconds,
            setting.dbSchema.AggregateData.getTimeStampLabel(), start,
            setting.dbSchema.AggregateData.getTimeStampLabel(), end,
            Tables.corrected_loop_data.induction_loop_id))
    # bind some variables used during insertion
    if setting.dbSchema.Loop.region_choices[0] == "leipzig":
        flowFactor = 1
    else:
        flowFactor = 3600 / intervalLength.seconds
    ## declare the minimum number of expected vehicles for maximum quality 
    ## at least on per aggregated interval, double for short intervals
    expectedEntryCount = intervalLength.seconds/updateInterval.seconds
    # if the aggregation interval is not a multiple of updateInterval we need to 
    # scale flows by the actual coverage of aggregated data rows
    def insert(intervalEnd):
        insertAggregated(conn, "loop", detReader,
                intervalEnd, intervalLength, flowScale=flowFactor,
                expectedEntryCount=expectedEntryCount)

    # aggregate data
    nextInterval = start + intervalLength
    for det, qPKW, qLKW, vPKW, vLKW, time, quality in rows:
        # XXX figure out whether this actually helps
        #if det not in validDetectors:
        #    quality = 0
        time = as_time(time)
        if time > nextInterval:
            insert(nextInterval)
            # advance interval, skipping gaps
            while time > nextInterval:
                nextInterval += intervalLength
        # register measurements, merges PKW and LKW
        if quality > 0:
            # both flows apply to the same time interval so rowCoverage only
            # needs to be incremented once
            if setting.getDetectorOptionBool("haslkw") and qPKW is not None and qLKW is not None and qLKW > 0:
                if qPKW > 0 and vPKW is not None and vLKW is not None:
                    vPKW = (vPKW * qPKW + vLKW * qLKW) / (qPKW + qLKW)
                else:
                    vPKW = vLKW
                qPKW += qLKW
        #    if det.startswith("SP0078-"):
        #        print(det, qPKW, vPKW, quality)
            detReader.addFlow(det, qPKW, vPKW, quality, 1.)
    # aggregate final (incomplete) interval
    #for d in ("SP0078-1","SP0078-2","SP0078-3"):
    #    data = detReader.getDetector(d)
    #    print(d, data.totalFlow, data.avgSpeed, data.quality, data.coverage, data.entryCount)
    insert(nextInterval)
    conn.close()


def _wait_if_trafficlight(row, waittime):
    """account for an everage waittime at edges which end at tls-controlled junctions"""
    # waiting at traffic light is already included in fcd speed
    # however it is distributed across the actual tls-edge and the succeding edges.
    # We assume that the speed is delayed by half the waittime.
    #
    # If we were interested in average edge speeds we should reduce
    # the speed on these edges further to account for the full average waittime.
    # When using these speeds to calibrate the simulation we must distinguish
    # between 2 cases:
    # 1) The simulation does not simulate junctions (mesosim default)
    #  - keep the speeds as they are because for whole routes they are correct on average
    #return row
    #  - reduce the speed before the tls since the whole waittime actually happens there
    time_correction = waittime.seconds / 2
    # 2) The simulation simulates junctions (microsim, mesosim with --meso-junction-control)
    #  - increase the speed before the tls because the waittime is simulated
    #time_correction = -waittime.seconds / 2

    edge, speed, time, coverage, veh, tlsFlag, edge_length = row
    if tlsFlag == 'trafficlight' and speed > 0:
        time_on_edge = edge_length / speed + time_correction
        if time_on_edge > 0: 
            new_speed = edge_length / time_on_edge
            row[1] = new_speed
    return row


def _getFilteredFCD(conn, start, end, waittime):
    Tables = setting.dbSchema.Tables
    if setting.dbSchema.Loop.region_choices[0] == "huainan":
        # traffic light handling disabled
        # length could be read from NET_EDGE_FBDandLENGTH_QGIS
        query = """
        SELECT e.%s, f.%s, f.%s, f.%s, f.veh_id, '', 0
        FROM %s f, %s e
        WHERE f.edge_id = e.%s AND f.%s > TIMESTAMP '%s' AND f.%s <= TIMESTAMP '%s'
              AND f.%s > 0
        ORDER BY f.veh_id, f.%s""" % (
            Tables.fbd_edge.fbd_id,
            Tables.floating_car_data.v_kfz,
            Tables.floating_car_data.data_time,
            Tables.floating_car_data.coverage,
            Tables.floating_car_data,
            Tables.fbd_edge,
            Tables.fbd_edge.edge_id,
            Tables.floating_car_data.data_time,
            start,
            Tables.floating_car_data.data_time,
            end,
            Tables.floating_car_data.coverage,
            Tables.floating_car_data.data_time)
    elif setting.dbSchema.Loop.region_choices[0] == "leipzig":
        query = """
        SELECT f.edge_id, f.%s, f.%s, f.%s * 100, NULL, NULL, NULL
        FROM %s f, %s e LEFT JOIN %s t ON e.edge_id = t.%s
        WHERE f.edge_id = e.edge_id AND
              f.%s > '%s' AND f.%s <= '%s'
              AND f.%s > 0
        ORDER BY f.%s""" % (
            Tables.floating_car_data.v_kfz,
            Tables.floating_car_data.data_time,
            Tables.floating_car_data.coverage,
            Tables.floating_car_data,
            Tables.edge,
            Tables.traffic_signal,
            Tables.traffic_signal.edge_id,
            Tables.floating_car_data.data_time,
            start,
            Tables.floating_car_data.data_time,
            end,
            Tables.floating_car_data.coverage,
            Tables.floating_car_data.data_time)
    else:
        query = """
        SELECT f.edge_id, f.%s, f.%s, f.%s, f.veh_id, CASE WHEN t.traffic_signal_id IS NULL THEN '' ELSE 'trafficlight' END, e.length
        FROM %s f, %s e LEFT JOIN %s t ON e.edge_id = t.%s
        WHERE f.edge_id = e.edge_id AND 
              f.%s > '%s' AND f.%s <= '%s'
              AND f.%s > 0
        ORDER BY f.veh_id, f.%s""" % (
            Tables.floating_car_data.v_kfz,
            Tables.floating_car_data.data_time,
            Tables.floating_car_data.coverage,
            Tables.floating_car_data,
            Tables.edge,
            Tables.traffic_signal,
            Tables.traffic_signal.edge_id,
            Tables.floating_car_data.data_time,
            start,
            Tables.floating_car_data.data_time,
            end,
            Tables.floating_car_data.coverage,
            Tables.floating_car_data.data_time)
    rows = database.execSQL(conn, query)
    if len(rows) == 0:
        return rows
    newRows = [list(rows[0])]
    for row in rows[1:]:
        # edge, speed, time, coverage, veh, tls, edge_length
        lastRow = newRows[-1]
        if row[0] == lastRow[0] and row[4] == lastRow[4] and row[4] is not None:
            # combine two sightings of the same vehicle on the same edge
            lastRow[1] = (lastRow[3] * lastRow[1] + row[3] * row[1]) / (row[3] + lastRow[3])
            lastRow[3] = min(100, lastRow[3] + row[3])
        else:
            # vehicle was seen only once on the last edge
            _wait_if_trafficlight(lastRow, waittime)
            newRows.append(list(row))
    lastRow = newRows[-1]
    _wait_if_trafficlight(lastRow, waittime)
    newRows.sort(key=operator.itemgetter(2)) # sort by data_time
    print('processing fcd values end')
    return newRows


def aggregateFCD(start, end, period, intervalLength, tlsWait):
    """Time aggregation of FCD in the given interval in subintervals
       of the given length."""  
    conn = database.createDatabaseConnection()
    detReaders = [DetectorReader()]
    nextInterval = start + intervalLength
    nextPeriod = start + period
    for edge, speed, time, coverage, veh, tls, edge_length in _getFilteredFCD(conn, start, end, tlsWait):
        time = as_time(time)
        while time > nextInterval and len(detReaders) > 0:
            insertAggregated(conn, "fcd", detReaders.pop(0),
                             nextInterval, intervalLength,
                             expectedEntryCount=intervalLength.seconds/600)
            nextInterval += period
        while time > nextPeriod:
            detReaders.append(DetectorReader())
            nextPeriod += period
        for detReader in detReaders:
            if not detReader.hasEdge(edge):
                detReader.addGroup(0, edge, MAX)
                detReader.addDetector(edge, 0, edge)
            # coverage of vehicles is averaged as quality indicator and summed for coverage statistic
            detReader.addFlow(edge, 1, speed, coverage, coverage / 100.0)
    insertAggregated(conn, "fcd", detReaders.pop(0), nextInterval,
                     intervalLength, doClose=True,
                     expectedEntryCount=intervalLength.seconds/600)


def generateComparison(outfile, time, types):
    data = defaultdict(dict)
    conn = database.createDatabaseConnection()
    reverseEdgeMap = reversedMap(setting.dbSchema.AggregateData.getSimulationEdgeMap(conn))
    for type in types:
        intervalTable, dataTable, q_column, v_column = setting.dbSchema.AggregateData.getSchema(type)
        if type in ('simulation', 'prediction'):
            # note: this assumes that no other connections are required afterward
            conn = database.createOutputConnection()
        rows = setting.dbSchema.AggregateData.getComparisonData(conn, type, time)
        for edge_id, q, v in rows:
            edge_id = reverseEdgeMap.get(edge_id, edge_id)
            data[edge_id][type] = (q, v)

    # write data to file
    with open(outfile, 'w') as out:
        print(time.strftime("%Y%m%d%H%M%S"), file=out)
        out.write("section-id")
        for type in types:
            out.write("\t%s-flow\t%s-speed" % (type, type))
        out.write("\n")
        for key, value in data.items():
            out.write(str(key))
            for type in types:
                out.write("\t%s\t%s" % value.get(type, ("NULL", "NULL")))
            out.write("\n")


def cleanUp(before, types, emission=False):
    conn = database.createOutputConnection()
    for type in types:
        if emission:
            intervalTable, dataTable, _ = setting.dbSchema.AggregateData.getEmissionSchema(type)
        else:
            intervalTable, dataTable, q_column, v_column = setting.dbSchema.AggregateData.getSchema(type)
        where = ""
        if before is None:
            try:
                # works only when the admin-right is available
                _, numDeletedItems = database.execSQL(conn, "TRUNCATE TABLE %s" % dataTable, True, returnRowcount=True)
            except:
                _, numDeletedItems = database.execSQL(conn, "DELETE FROM %s" % dataTable, True, returnRowcount=True)
        else:
            where = "WHERE  %s <= %s '%s' %s" % (
                intervalTable.traffic_time,
                setting.dbSchema.AggregateData.getTimeStampLabel(),
                before,
                setting.dbSchema.Extrapolation.getTypePredicate(type)
            )
            query = "DELETE FROM %s WHERE %s IN (SELECT %s FROM %s %s)" % (
                dataTable,
                setting.dbSchema.Tables.traffic.traffic_id,
                setting.dbSchema.Tables.traffic.traffic_id,
                intervalTable,
                where
            )
            _, numDeletedItems = database.execSQL(conn, query, True, returnRowcount=True)
        print(("deleted %s items from %s" % (numDeletedItems, dataTable)))
        _, numDeletedIntervals = database.execSQL(conn, """
            DELETE FROM %s %s""" % (intervalTable, where), True, returnRowcount=True)
        print(("deleted %s intervals of type %s" % (numDeletedIntervals, type)))
