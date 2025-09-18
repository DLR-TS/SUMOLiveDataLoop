# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    generateEmissionOutput.py
# @author  Michael Behrisch
# @date    2007-06-07
"""
A XML ContentHandler which parses a SUMO emission file for NOx, CO2, CO, HC, PMx on
edges and writes the results to the DB. Usually it is called from *.py.
"""
import csv
import datetime
import gzip
import sys

from . import setting
from . import database

class EmissionReader:
    """ContentHandler for parsing SUMO emission outputs.
       It automatically parses the file given to the constructor."""

    def __init__(self, emissionfile, emissionInterpretation, emissionNormed):
        self._emissionInterpretation = emissionInterpretation
        self._out = {} # file objects for outfiles for the dumpID
        self._detReader = {} # one detector reader for every dumpID
        self._aggregation = None # the length of the currently parsed interval in seconds
        if emissionInterpretation:
            for id in emissionInterpretation.keys():
                self._detReader[id] = []
        with gzip.open(emissionfile, "rt") as input:
            for data in csv.DictReader(input, delimiter=";"):
                interval_id = data['interval_id']
                start = float(data['interval_begin'])
                end = float(data['interval_end'])
                if interval_id not in self._emissionInterpretation:
                    print("WARNING: found unknown emission data interval '%s'" % interval_id, file=sys.stderr)
                    continue
                self._aggregation = end - start
                edge = data['edge_id']
                CO = float(data['edge_CO_normed' if emissionNormed else 'edge_CO_abs'])
                CO2 = float(data['edge_CO2_normed' if emissionNormed else 'edge_CO2_abs'])
                HC = float(data['edge_HC_normed' if emissionNormed else 'edge_HC_abs'])
                PMx = float(data['edge_PMx_normed' if emissionNormed else 'edge_PMx_abs'])
                NOx = float(data['edge_NOx_normed' if emissionNormed else 'edge_NOx_abs'])
                self._detReader[interval_id].append((edge, (NOx, CO, PMx, HC, CO2)))

    def updateDB(self, intervalLength=None, base=None):
        if intervalLength is None:
            intervalLength = datetime.timedelta(seconds=self._aggregation)
        if self._emissionInterpretation:
            for id, (time, trafficType, filename) in list(self._emissionInterpretation.items()):
                insertEmission(None, trafficType, self._detReader[id], time, intervalLength)


def interpret_emission(emissionfile, intervalLength, emissionInterpretation, emissionNormed):
    emissionReader = EmissionReader(emissionfile, emissionInterpretation, emissionNormed)
    emissionReader.updateDB(intervalLength)


def insertEmission(conn, typeName, detReader, intervalEnd, intervalLength):
    """Insert emission data into the database. The data is read from the
       given DetectorReader. If it is simulation data the edge ids are
       taken as Navteq IDs instead of database road_section IDs and scenarios are taken into account."""
    AggregateData = setting.dbSchema.AggregateData
    if conn == None:
        conn = database.createOutputConnection()
        doClose = True
    trafficIndex = None
    values = {}
    edgeMap = AggregateData.getSimulationEdgeMap(conn, True) # edgeMap['sumo_id']=['fbd_id1',....]

    # handle existing entry in traffic for the same type and time
    trafficIndex = AggregateData.getIntervalID(conn, typeName, intervalEnd, intervalLength, True)
    # process the detReader
    totalQuality = 0.
    unknownEdges = 0
    for edge, emissions in detReader:
        if edge not in edgeMap:
            unknownEdges += 1
            if unknownEdges < 10:   # avoid to generate too much output 
                sys.stderr.write("Ignoring data for unknown simulation edge '%s'\n" % edge) 
            continue
        else:
            edgeList = edgeMap[edge] # make edge a list of database-ids
        for database_id in edgeList:
            NOx, CO, PMx, HC, CO2, entryCount = values.get(database_id, (0, 0, 0, 0, 0, 0))  # todo: Elmar: should the absent data be filled with 0 or none? or no action?
            # sum values for all groups
            NOx += emissions[0]
            CO += emissions[1]
            PMx += emissions[2]
            HC += emissions[3]
            CO2 += emissions[4]
            entryCount += 1
            values[database_id] = (NOx, CO, PMx, HC, CO2, entryCount)
    insertRows = []
    for edge, (NOx, CO, PMx, HC, CO2, entryCount) in list(values.items()):
        if entryCount > 0:
            insertRows.append((trafficIndex, edge, NOx, CO, PMx, HC, CO2, None))
    # perform DB write
    if unknownEdges > 0:
        sys.stderr.write("Ignored data for %s unknown simulation edges'\n" % unknownEdges)
    if len(insertRows) > 0:
        AggregateData.insertEmissionData(conn, typeName, insertRows)
