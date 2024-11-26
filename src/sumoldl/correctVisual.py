# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    correctVisual.py
# @author  Michael Behrisch
# @date    2008-12-16
"""
Corrects and aggregates traffic data from evaluated pictures
"""
import setting, database
from datetime import datetime, timedelta
from collections import defaultdict

from aggregateData import insertAggregated
from evalDetector import Data
from detector import DetectorReader


def correctVisual(correctStart, correctEnd):
    """
    Correct visual data values by checking for obvious errors.
    """ 
    error_counts = defaultdict(int)
    conn = database.createDatabaseConnection()
    rows = database.execSQL(conn, """
        SELECT visual_data_id, detection_time, vehicle_density, 0, average_speed, 0, edge_id
        FROM visual_data v
        WHERE detection_time >= '%s' AND detection_time < '%s'""" % (correctStart, correctEnd))
    values = []
    for row in rows:
        data = Data(row[0], row[6], row[1], None, None, row[4], row[5])
        vPKW = row[4]
        if vPKW == 0:
            vPKW = 50 / 3.6
        if row[2] == None:
            data.qPKW = 0
        else:
            data.qPKW = 3.6 * row[2] / vPKW
        vLKW = row[5]
        if vLKW == 0:
            vLKW = 50 / 3.6
        if row[3] == None:
            data.qLKW = 0
        else:
            data.qLKW = 3.6 * row[3] / vLKW
        data.check(timedelta(hours=1))
        # update error counts
        for a in Data.attrs:
            if getattr(data, a) is None:
                error_counts[a] += 1
        v = data.toValues(row[1])
        if v:
            values.append(v)
        data.toBeWritten = False
    if values:
        command = """INSERT INTO corrected_visual_data(original_data_id, edge_id, data_time,
                                                       q_pkw, q_lkw, v_pkw, v_lkw,
                                                       quality) VALUES """ + (",".join(values))
        database.execSQL(conn, command, True)
    conn.close()
    summary = "db-lines read: %s, written %s" % (len(rows), len(values))
    header = 'attr\terrors'
    entries = ['\t'.join(map(str, [a, error_counts[a]])) for a in Data.attrs]
    print '\n'.join([summary, header] + entries)

def aggregateVisual(start, end, intervalLength):
    """Time aggregation of visual data in the given interval in subintervals
       of the given length."""  
    conn = database.createDatabaseConnection()
    detReader = DetectorReader()
    rows = database.execSQL(conn, """
        SELECT edge_id, q_pkw, q_lkw, v_pkw, v_lkw, data_time, quality
        FROM corrected_visual_data WHERE data_time >= '%s' AND data_time < '%s'
        ORDER BY data_time""" % (start, end))
    nextInterval = start + intervalLength
    for row in rows:
        cmpTime = database.as_time(row[5])
        if cmpTime >= nextInterval:
            insertAggregated(conn, "visual", detReader,
                             nextInterval, intervalLength)
            while cmpTime >= nextInterval:
                nextInterval += intervalLength
        if not detReader.hasEdge(row[0]):
            detReader.addGroup(0, row[0])
            detReader.addDetector(row[0], 0, row[0])
        if row[1]:
            detReader.addFlow(row[0], row[1], row[3], row[6])
        if row[2]:
            detReader.addFlow(row[0], row[2], row[4], row[6])
    insertAggregated(conn, "visual", detReader, nextInterval,
                     intervalLength, doClose=True)
