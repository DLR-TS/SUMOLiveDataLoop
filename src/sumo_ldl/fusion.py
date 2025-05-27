# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    fusion.py
# @author  Jakob Erdmann
# @author  Michael Behrisch
# @date    2012-07-23
"""
Helper functions for data fusion
Used by correctDetector
"""

import os,sys
from datetime import datetime
from collections import defaultdict
from itertools import groupby, chain

from . import database
from .detector import DetectorReader
from .aggregateData import insertAggregated
from .setting import getOptionBool, dbSchema

QUALITY_FACTOR = {
        'loop' : 1.0,
        'fcd'  : 0.5, # we do not trust fcd quality fully, because only a fraction of all vehicles is equipped
        }

class FusionValue:
    def __init__(self):
        self.initialized = False
        self.weightedSum = 0
        self.inverseQuality = 1.0 # 1 - quality
        self.weight = 0

    def add(self, value, qualityPercent, weight):
        # stop fusioning if we already have a high quality value
        if value is not None and self.inverseQuality > 0:
            self.initialized = True
            self.weightedSum += weight * value
            self.weight += weight
            self.inverseQuality *= (1 - qualityPercent / 100.0)

    def getValueAndQualityPercent(self):
        value = None
        qualityPercent = None
        if self.initialized and self.weight > 0:
            value = self.weightedSum / self.weight
            qualityPercent = 100 * (1 - self.inverseQuality)
        return value, qualityPercent


def fusion(start, end, intervalLength):
    """Main entry point of this module. 
    Wrapper calling the real fusion repeatedly for each interval."""
    detReader = DetectorReader()
    conn = database.createDatabaseConnection()
    fusionTime = start
    while fusionTime <= end:
        fusionTime += intervalLength
        _fusion(conn, detReader, fusionTime, intervalLength)
        insertAggregated(conn, "fusion", detReader, fusionTime, intervalLength)
    conn.close()

def _fusion(conn, detReader, intervalEnd, intervalLength):
    """XXX care must be taken not to average the fcd-count with the detector flow

    Read traffic data from DB and add data to the given detReader.
    Fusion according to quality"""
    rows = {}
    for source in ("loop", "fcd"):
        intervalTable, dataTable, qCol, vCol = dbSchema.AggregateData.getSchema(source)
        query = """SELECT edge_id, '%s', %s, %s, d.quality
            FROM %s i, %s d WHERE i.%s = d.%s AND
            i.%s > '%s' AND i.%s <= '%s'
            ORDER BY edge_id, d.quality""" % (
            source, qCol, vCol, intervalTable, dataTable,
            intervalTable.traffic_id, intervalTable.traffic_id,
            intervalTable.traffic_time, intervalEnd - intervalLength,
            intervalTable.traffic_time, intervalEnd)
        rows[source] = database.execSQL(conn, query)

    for edge, subrows in groupby(chain(rows["loop"], rows["fcd"]), lambda x:x[0]):
        qFusion = FusionValue()
        vFusion = FusionValue()
        for edge, source, q, v, quality in subrows:
            # since multiple fcd intervals may cover this fusion interval, we reduced their relative weight
            adaptedQuality = quality * QUALITY_FACTOR[source]
            adaptedWeight = adaptedQuality #* intervalLength.seconds / aggregation_interval
            # using quality as fusion weight
            if source != "fcd":  # fcd counts aren't flows
                qFusion.add(q, adaptedQuality, adaptedWeight)
            vFusion.add(v, adaptedQuality, adaptedWeight)
        flow, flowQual = qFusion.getValueAndQualityPercent()
        speed, speedQual = vFusion.getValueAndQualityPercent()
        # fix inconsistent values
        if flow == 0 and (speed or 0) > 0:
            flow = 1
        if speed == 0 and (flow or 0) > 0:
            speed = None
        quality = max(flowQual or 0, speedQual or 0)
        if quality > 0:
            if not detReader.hasEdge(edge):
                detReader.addGroup(0, edge)
                detReader.addDetector(edge, 0, edge)
            detReader.addFlow(edge, flow, speed, quality)
