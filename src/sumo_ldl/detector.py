#!/usr/bin/env python
# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    detector.py
# @author  Michael Behrisch
# @date    2007-06-26
"""
Helper classes for parsing XML detector descriptions and collecting
flows and speeds on detectors.
Helper functions for parsing Elmar-detector files and database
detectors.
"""

import os
import sys, optparse, datetime
from itertools import groupby
import codecs
from xml.sax import parse, handler, saxutils

from .tools import reversedMap
from . import setting

MAX_POS_DEVIATION = 10

AVERAGE = 1
MAX = 2

def import_database_modules(schema):
    global pgdb
    global cx_Oracle
    global dbSchema
    global database
    try:
        import psycopg2 as pgdb
    except ImportError:
        import pgdb
    try:
        import cx_Oracle
    except ImportError:
        print("Warning! Oracle client is not available.", file=sys.stderr)
    if schema is None:
        sys.exit("Option schema is mandatory")
    if schema[-3:] == '.py':
        schema = schema[:-3]
    sys.path.append(os.path.dirname(schema))
    dbSchema =  __import__(os.path.basename(schema))
    import setting
    setting.dbSchema = dbSchema
    import database


class Detector:
    OPTIONAL_FIELDS = [
            'description',
            'vendor',
            'direction_of_traffic']

    def __init__(self, id, lane, interval, **attrs):
        self.id = id
        self.lane = lane # 1-based
        self.interval = interval
        for a in Detector.OPTIONAL_FIELDS:
            setattr(self, a, None)
        for a, value in list(attrs.items()):
            if a in Detector.OPTIONAL_FIELDS:
                setattr(self, a, value)
            else:
                print("Warning: unknown detector attribute '%s'" % a)


class DetectorGroupData:

    OPTIONAL_FIELDS = [
            'id',
            'description',
            'next_location_code',
            'next_location_name',
            'loop_type',
            'road_km',
            'road_name',
            'data_source']
    """
    Storage for data in a detector group
    """
    def __init__(self, pos, qualityMeasure=AVERAGE):
        self.detectors = []
        self.groupID = None
        self.pos = pos
        self.latitude = 0
        self.longitude = 0
        self.streetType = "highway" 
        self.qualityMeasure = qualityMeasure
        # optional fields (not supported by all db-schemas)
        for a in DetectorGroupData.OPTIONAL_FIELDS:
            setattr(self, a, None)
        self.reset()

    def reset(self):
        """
        Reset flow, speed and quality values
        """
        self.totalFlow = None
        self.avgSpeed = None
        self.quality = None
        self.coverage = None
        self.entryCount = 0

    def addDetector(self, detID, lane, interval, **attrs):
        """
        Add a detector to the group
        """
        self.detectors.append(Detector(detID, lane, interval, **attrs))

    def _updateSpeed(self, speed, oldWeight, newWeight):
        """updated avgSpeed with weighted average of old and new speed value"""
        if speed is not None:
            if self.avgSpeed is None:
                self.avgSpeed = 0 # init speed on first usable data point
            self.avgSpeed = (self.avgSpeed * oldWeight + speed * newWeight) / (oldWeight + newWeight)

    def addDetFlow(self, flow, speed=None, quality=0, coverage=None):
        """
        Add a flow value and optionally speed and quality values to this group.
        Flow is simply summed, speed is averaged and quality is either
        average or maximum depending on the measure given at creation.
        """
        # update flow and speed
        if flow is not None and self.totalFlow is None:
            self.totalFlow = 0 # init totalFlow on first usable data point
        if flow: # do not add zero flow
            self._updateSpeed(speed, self.totalFlow, flow)
            self.totalFlow += flow
        elif flow is None and speed is not None:
            # FCD data
            self._updateSpeed(speed, self.entryCount, 1)
        # update quality
        if self.quality is None and quality is not None:
            self.quality = 0 # init quality on first usable data point
        elif self.quality is not None and quality is None:
            quality = 0 # patch for averaging
        if self.quality is not None and quality is not None:
            if (self.qualityMeasure == AVERAGE):
                self.quality = (self.quality * self.entryCount + quality) / (self.entryCount + 1)
            elif (self.qualityMeasure == MAX):
                self.quality = max(self.quality, quality)
        self.entryCount += 1
        # udpate coverage
        if self.coverage is None and coverage is not None:
            self.coverage = 0 # init coverage on first usable data point
        if coverage:
            self.coverage += coverage

    def __repr__(self):
        return str(self.ids)


class DetectorReader(handler.ContentHandler):
    """
    Collection of detector groups together with methods for adding groups,
    detectors and flow and parsing / writing the appropriate XML-file.
    """

    def __init__(self, detFile=None, dbSchemaFile=None):
        handler.ContentHandler.__init__(self)
        self.hasIDs = 0
        self._edge2DetData = {}
        self._det2edge = {}
        self._det2group = {}
        self._currentGroup = None
        self._currentEdge = None
        if detFile:
            parse(detFile, self)
        if dbSchemaFile is not None:
            import_database_modules(dbSchemaFile)


    def printDetectors(self, guessLanes=False, file=sys.stdout):
        """
        Write XML-description of the detectors stored in a format readable by
        SUMO's dfrouter.
        """
        print("""<?xml version="1.0" encoding="ISO-8859-1"?>
<!-- generated on %s by $Id: detector.py 9186 2021-03-16 15:11:10Z wang_yu $ -->
<a>""" % datetime.datetime.now(), file=file)
        for edge in sorted(list(self._edge2DetData.keys()), key=str):
            for group in self._edge2DetData[edge]:
                file.write('    <group pos="%s"' % group.pos)
                if group.latitude:
                    file.write(' lat="%s" lon="%s"' % (group.latitude, group.longitude))
                for attr in DetectorGroupData.OPTIONAL_FIELDS:
                    if getattr(group, attr): 
                        file.write(' %s="%s"' % (attr, saxutils.escape(getattr(group, attr))))
                file.write(' street_type="%s" orig_edge="%s">\n' % (group.streetType, edge))
                if guessLanes:
                    minLane = 9
                    for d in group.detectors:
                        try:
                            minLane = min(minLane, int(d.id[-1]))
                        except:
                            minLane = None
                            break
                    if minLane is not None:
                        for d in group.detectors:
                            d.lane = int(d.id[-1]) - minLane + 1

                for detector in group.detectors:
                    laneString = ""
                    if detector.lane != None: 
                        laneString = 'lane="%s_%s" ' % (edge, detector.lane-1)
                    optional = ""
                    for attr in Detector.OPTIONAL_FIELDS:
                        if getattr(detector, attr) is not None:
                            optional += ' %s="%s"' % (attr, getattr(detector, attr))
                    file.write('        <detector_definition id="%s"' % saxutils.escape(detector.id))
                    file.write(' %spos="%s" interval="%s"%s/>\n' % (laneString, group.pos, detector.interval, optional))
                file.write("    </group>\n")
        file.write("</a>\n")

    def writeDetectorDB(self, conn, clean=True):
        """
        Write detectors to the database.
        """
        if clean:
            database.execSQL(conn, "DELETE FROM %s" % dbSchema.Tables.induction_loop, True)
            if hasattr(dbSchema.Tables, 'induction_loop_group_edge'):
                database.execSQL(conn, "DELETE FROM %s" % dbSchema.Tables.induction_loop_group_edge, True)
            database.execSQL(conn, "DELETE FROM %s" % dbSchema.Tables.induction_loop_group, True)
            if not hasattr(dbSchema.Detector, 'getLastGroupID'):  # sequences available
                database.execSQL(conn, "SELECT setval('%s_induction_loop_id_seq', 1, false)" % dbSchema.Tables.induction_loop, True)
                database.execSQL(conn, "SELECT setval('%s_induction_loop_group_id_seq', 1, false)" % dbSchema.Tables.induction_loop_group, True)
        edgeMap = dbSchema.AggregateData.getSimulationEdgeMap(conn)
        for edge, groups in list(self._edge2DetData.items()):
            if not edge in edgeMap:
                print("Skipping detector for unknown edge %s" % edge, file=sys.stderr)
                continue
            edge = edgeMap[edge]
            for group in groups:
                command = dbSchema.Detector.insert_induction_loop_group_query(edge, group)
                fetchId = True
                if hasattr(dbSchema.Detector, 'getLastGroupID'):
                    fetchId = dbSchema.Detector.getLastGroupID
                if isinstance(command, str):
                    row = database.execSQL(conn, [command], doCommit=True, fetchId=fetchId)
                else:  # it is only for the project Huainan
                    row = database.execSQL(conn, [command[0]], doCommit=True, fetchId=fetchId)
                    database.execSQL(conn, [command[1]], doCommit=True)
                if row:
                    groupID = row[0]
                    for detector in group.detectors:
                        if detector.lane is None:
                            detector.lane = 1
                        database.execSQL(conn,
                                dbSchema.Detector.insert_induction_loop_query(groupID, detector), True)
                else:
                    print("Warning! Detector group %s (section %s) could not be inserted." % (group.description, edge), file=sys.stderr)

    def addDetector(self, detID, pos, edge, lane=None, interval=None, lon=None, lat=None, type=None, **attrs):
        """
        Add a detector to this collection. If there is a current group, it is
        appended to that group. If not but it is on an edge nearby another
        detector (<= MAX_POS_DEVIATION) it is added to its group.
        Otherwise a new group is created, which is _not_ the current group
        afterwards.
        """
        if detID in self._det2edge:
            print("Warning! Detector %s already known." % detID, file=sys.stderr) 
            return
        group = self._currentGroup
        if group == None:
            if not edge in self._edge2DetData:
                self._edge2DetData[edge] = []
            for data in self._edge2DetData[edge]:
                if abs(data.pos - pos) <= MAX_POS_DEVIATION:
                    group = data
                    break
        if group == None:
            group = DetectorGroupData(pos)
            if lon:
                group.longitude = lon
            if lat:
                group.latitude = lat
            if type:
                group.streetType = type
            self._edge2DetData[edge].append(group)
        if not interval:
            interval = "60"
        group.addDetector(detID, lane, interval, **attrs)
        self._det2edge[detID] = edge
        self._det2group[detID] = group

    def addGroup(self, pos, edge, qualityMeasure=AVERAGE):
        """
        A new group is created, which is the current group hereafter.
        """
        self._currentGroup = DetectorGroupData(pos, qualityMeasure)
        self._currentEdge = edge
        if not edge in self._edge2DetData:
            self._edge2DetData[edge] = []
        self._edge2DetData[edge].append(self._currentGroup)
        return self._currentGroup
        
    def startElement(self, name, attrs):
        """
        XML-Handler function for parsing XML-descriptions.
        """
        if name == 'detector_definition':
            if self._currentEdge:
                edge = self._currentEdge
            else:
                edge = attrs['lane'][:-2]
            lane = None
            if 'lane' in attrs:
                lane = int(attrs['lane'][-1]) + 1
            interval = None
            if 'interval' in attrs:
                interval = attrs['interval']
            # optional attributes
            optional = {}
            for attr in Detector.OPTIONAL_FIELDS:
                if attr in attrs:
                    optional[attr] = attrs[attr]
            self.addDetector(attrs['id'], float(attrs['pos']), edge, lane, interval, **optional)
        elif name == 'group':
            group = self.addGroup(float(attrs['pos']), attrs['orig_edge'])            
            if 'group_id' in attrs:
                group.groupID = int(attrs['group_id'])
            if 'lat' in attrs:
                group.latitude = float(attrs['lat']) 
                group.longitude = float(attrs['lon']) 
            if 'street_type' in attrs:
                group.streetType = attrs['street_type']
            for attr in DetectorGroupData.OPTIONAL_FIELDS:
                if attr in attrs:
                    setattr(group, attr, attrs[attr])
        elif name == 'a':
            if 'with_id' in attrs:
                if attrs['with_id'] == 'true':
                    self.hasIDs = 1
                else:
                    self.hasIDs = 0

    def endElement(self, name):
        """
        XML-Handler function for parsing XML-descriptions.
        """
        if name == 'group':
            self._currentGroup = None
            self._currentEdge = None

    def addFlow(self, det, flow, speed, quality=0, coverage=None):
        """
        Add flow to the given detector. If the detector is unknown, nothing happens.
        """
        if det in self._det2group:
            self._det2group[det].addDetFlow(flow, speed, quality, coverage)

    def setFlow(self, edge, flow, speed):
        """
        Set flow for all detector groups on the given edge.
        If the edge is unknown, nothing happens.
        """
        if edge in self._edge2DetData:
            for group in self._edge2DetData[edge]:
                group.reset()
                group.addDetFlow(flow, speed)

    def getEdgeDataIterator(self):
        """
        Iterator over pairs of edge and detector groups with associated data.
        """
        return list(self._edge2DetData.items())
    
    def hasIDs(self):
        """
        Return whether the input file has id or not
        """
        return self.hasIDs()
    
    def hasEdge(self, edge):
        """
        Returns whether the edge has detector groups.
        """
        return edge in self._edge2DetData

    def getMaxGroupSize(self, edge):
        """
        Returns the maximum number of detectors in a group of the edge.
        """
        maxSize = 0
        for data in self._edge2DetData[edge]:
            if len(data.detectors) > maxSize:
                maxSize = len(data.detectors)
        return maxSize
    
    def getDetector(self, id):
        return self._det2group[id]

def readDetectors(detectorFilename, edges=None):
    """
    Parse detectors from Elmar's point collection.
    It has a white space separated format, where the second entry equal to "5"
    indicates a detector, the third entry is the detector description
    and the sixth entry is the edge id.
    The detector description is a ";"-separated list containing the name
    at the first position and optionally the position on the edge
    at the last but one position (starting with "DISTANCE").
    """
    detReader = DetectorReader()
    detectorFile = codecs.open(detectorFilename, "r", "latin1")
    for l in detectorFile:
        if l[0] != '#':
            detDef = l.strip().split("\t")
            if len(detDef) < 6 or detDef[1] not in ["5", "6"]:
                continue
            edge = detDef[5]
            lon = float(detDef[3])/100000.
            lat = float(detDef[4])/100000.
            type = None
            if detDef[1] == "6":
                type = "urban"
            detName = detDef[2].split(';')
            pos = 0.0
            for entry in detName[1:]:
                if entry.startswith('DISTANCE'):
                    pos = float(entry[8:])
            if edges:
                if edge not in edges:
                    print("Warning! Unknown edge %s." % edge, sys.stderr)
                else:
                    for splitEdge in edges[edge]:
                        if pos < splitEdge._start + splitEdge._length:
                            edge = splitEdge._id
                            pos -= splitEdge._start
                            break                    
                    if pos > splitEdge._start + splitEdge._length:
                        print("Warning! Invalid detector pos on %s." % edge, sys.stderr)
                        edge = splitEdge._id
                        pos = splitEdge._start + splitEdge._length
            detReader.addDetector(detName[0], pos, edge, lon=lon, lat=lat, type=type)
    detectorFile.close()
    return detReader

def readDetectorDB(conn, dismissWhenNoLane=False):
    """
    Read the detectors from a database
    """
    detReader = DetectorReader()
    command = """
       SELECT i.induction_loop_group_id, data_id, lane_no, i.description, 
       i.%s, edge_id, g.%s, street_type, st_astext(g.%s)
       FROM %s i, %s g
       WHERE TRUE
       AND i.induction_loop_group_id = g.induction_loop_group_id
       ORDER BY i.induction_loop_group_id""" % (
               dbSchema.Tables.induction_loop.loop_interval,
               dbSchema.Tables.induction_loop_group.position,
               dbSchema.Tables.induction_loop_group.geom_wgs84,
               dbSchema.Tables.induction_loop,
               dbSchema.Tables.induction_loop_group)
    rows = database.execSQL(conn, command)
    reverseEdgeMap = reversedMap(dbSchema.AggregateData.getSimulationEdgeMap(conn))
    for group_id, subrows in groupby(rows, lambda x:x[0]):
        subrows = list(subrows)
        group_id, data_id, lane_no, description, loop_interval, navteq_id, position, street_type, point = subrows[0]
        group = detReader.addGroup(position, navteq_id)
        group.description = description
        group.streetType = street_type
        for row in subrows:
            group_id, data_id, lane_no, description, loop_interval, edge_id, position, street_type, point = row
            edge_id = reverseEdgeMap.get(edge_id, edge_id)
            group.longitude, group.latitude = database.as_lon_lat(point)
            detReader.addDetector(data_id, position, edge_id, lane_no, loop_interval)
    return detReader


class Edge:

    def __init__(self, edgeID):
        self._id = edgeID
        self._length = -1
        self._numLanes = 0
        self._start = 0.0
        if "." in edgeID:
            self._start = float(edgeID[edgeID.index(".")+1:])

    def __repr__(self):
        return self._id

class EdgeReader(handler.ContentHandler):

    def __init__(self, netFile):
        handler.ContentHandler.__init__(self)
        self._currentEdge = None
        self._edges = {}
        parse(netFile, self)

    def getEdges(self):
        return self._edges

    def startElement(self, name, attrs):
        """
        XML-Handler function for parsing XML-descriptions.
        """
        if name == 'edge':
            self._currentEdge = Edge(attrs['id'])
        elif name == 'lane':
            self._currentEdge._length = float(attrs['length'])
            self._currentEdge._numLanes += 1

    def endElement(self, name):
        """
        XML-Handler function for parsing XML-descriptions.
        """
        if name == 'edge':
            id = self._currentEdge._id
            if "." in id:
                id = id[:id.index(".")]
            if id not in self._edges:
                self._edges[id] = [self._currentEdge]
            else:
                edges = self._edges[id]
                idx = 0
                while idx < len(edges) and edges[idx]._start < self._currentEdge._start:
                    idx += 1
                edges.insert(idx, self._currentEdge)
            self._currentEdge = None


def connect(args, engine):
    dbargs = args.split(":")
    if len(dbargs) > 4:
        port = dbargs[4]
    else:
        if engine == "postgres":
            port = "5432"
        else:
            port = "1521"  # oracle
    if engine == "postgres":
        return pgdb.connect(host=dbargs[2], user=dbargs[0], password=dbargs[1], database=dbargs[3])
    else:
        return cx_Oracle.connect(dbargs[0], dbargs[1], cx_Oracle.makedsn(dbargs[2], port, service_name=dbargs[3]))


def main():
    options = get_options()
    """
    Parse detector information either from db or file depending on the
    argument given and output the resulting detectors as XML.
    """
    edges = None
    if options.netfile:
        if options.verbose:
            print("Reading %s." % options.netfile, file=sys.stderr)
        edges = EdgeReader(options.netfile).getEdges()
    if options.detfile:
        if options.verbose:
            print("Reading %s." % options.detfile, file=sys.stderr)
        detReader = DetectorReader(options.detfile)
    elif options.elmfile:
        if options.verbose:
            print("Reading %s." % options.elmfile, file=sys.stderr)
        detReader = readDetectors(options.elmfile, edges)
    else:
        import_database_modules(options.schema)
        if options.detconn is None:
            options.detconn = dbSchema.Detector.detconn_default
        conn = connect(options.detconn, options.database_engine)
        detReader = readDetectorDB(conn, options.dismissNoLane)
        conn.close()
    if options.outconn:
        import_database_modules(options.schema)
        conn = connect(options.outconn, options.database_engine)
        detReader.writeDetectorDB(conn, options.clean) 
        conn.close()
    else:
        detReader.printDetectors(options.guessLanes)


def get_options():
    optParser = optparse.OptionParser()
    optParser.add_option("-d", "--detector-db", dest="detconn",
                         help="read detectors from database connection",
                         metavar="user:passwd:host:db[:port]")
    optParser.add_option("-f", "--detector-file", dest="detfile",
                         help="read detectors from XML FILE",
                         metavar="FILE")
    optParser.add_option("-e", "--elmar-file", dest="elmfile",
                         help="read detectors from Elmar FILE",
                         metavar="FILE")
    optParser.add_option("-n", "--net-file", dest="netfile",
                         help="sumo net FILE to validate edges against",
                         metavar="FILE")
    optParser.add_option("-o", "--detector-output-db", dest="outconn",
                         help="write detectors to database connection",
                         metavar="user:passwd:host:db[:port]")
    optParser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                         default=False, help="tell me what you are doing")
    optParser.add_option("-c", "--no-clean", action="store_false", dest="clean",
                         default=True, help="do not clean database before writing detectors")
    optParser.add_option("--dismiss-when-lane-undefined", action="store_true", dest="dismissNoLane",
                         default=False, help="do not read detectors from db that have no lane assigned")
    optParser.add_option("-g", "--guess-lanes", action="store_true", dest="guessLanes",
                         default=False, help="try to guess missing lane information from detector name")
    optParser.add_option("-s", "--schema", help="load the db schema from the given python file")
    optParser.add_option("-E", "--database-engine", type="choice", choices=["oracle", "postgres"],
                         default="oracle", help="choose the database engine either oracle or postgres")
    options, args = optParser.parse_args()
    return options

if __name__ == '__main__':
    main()
