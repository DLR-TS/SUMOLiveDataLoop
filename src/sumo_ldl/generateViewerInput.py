#!/usr/bin/env python
# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    generateViewerInput.py
# @author  Michael Behrisch
# @date    2007-06-07
"""
A XML ContentHandler which parses a SUMO dumpfile for speeds and flows on
edges and writes the results to a plain text file for Elmar's Viewer and
to the DB. Usually it is called from runStep.py.
"""
import csv
import datetime
import gzip
import os
import sys
from optparse import OptionParser

from .detector import DetectorReader
from .aggregateData import insertAggregated
from . import setting


class DumpReader:
    """ContentHandler for parsing SUMO dumps.
       It automatically parses the file given to the constructor."""

    def __init__(self, dumpfile, dumpInterpretation):
        self._dumpInterpretation = dumpInterpretation
        self._out = {} # file objects for outfiles for the dumpID
        self._detReader = {} # one detector reader for every dumpID
        self._aggregation = None # the length of the currently parsed interval in seconds
        if dumpInterpretation:
            for id, (time, traffic_type, filename) in dumpInterpretation.items():
                if filename is not None:
                    out = open(filename, 'w')
                    print(time.strftime("%Y-%m-%d %H:%M"), file=out)
                    print("Navtech-ID\tm/s\tveh/h", file=out)
                    self._out[id] = out
                self._detReader[id] = DetectorReader()
        with gzip.open(dumpfile, "rt") as input:
            for data in csv.DictReader(input, delimiter=";"):
                interval_id = data['interval_id']
                start = float(data['interval_begin'])
                end = float(data['interval_end'])
                if interval_id not in dumpInterpretation:
                    print("WARNING: found unknown dump interval '%s'" % interval_id, file=sys.stderr)
                    continue
                self._aggregation = end - start
                edge, num_vehs, speed = DumpReader.interpretEdge(data)
                if num_vehs > 0 and not 'Added' in edge and speed is not None:
                    if interval_id in self._out:
                        print("%s\t%i\t%i" % (
                                edge, speed, num_vehs * 3600 / self._aggregation), file=self._out[interval_id])
                    detReader = self._detReader[interval_id]
                    if not detReader.hasEdge(edge):
                        detReader.addGroup(0, edge)
                        detReader.addDetector(edge, 0, edge)
                    # transform simulation speeds (m/s) into db speeds (m/s or km/h) depending on dbSchema
                    detReader.addFlow(edge, num_vehs, speed * 3.6 / setting.dbSchema.EvalDetector.kmhMultiplier)
        for out in self._out.values():
            out.close()

    def updateDB(self, intervalLength=None, base=None):
        if intervalLength is None:
            intervalLength = datetime.timedelta(seconds=self._aggregation)
        if self._dumpInterpretation:
            for id, (time, trafficType, filename) in self._dumpInterpretation.items():
                insertAggregated(None, trafficType, 
                        self._detReader[id], time, intervalLength, True,
                        flowScale=3600/intervalLength.seconds)


    @staticmethod
    def interpretEdge(attrs):
        """return edge_id, vehPerHour, speed for the given edge attributes"""
        edge = attrs['edge_id']
        if not attrs.get('edge_speed'):
            return edge, 0, None
        speed = float(attrs['edge_speed'])
        # departed + entered = driving + arrived + left (left includes vaporized due to calibration)
        # vehicles removed due to calibration should not be counted here
        # 1) departed + entered - vaporized  <-> detector at the start of the edge
        # 2) arrived + left - vaporized      <-> detector at the end of the edge
        # calibrators use definition 1 so we do the same here for consistency
        num_vehs = float(attrs['edge_departed']) + float(attrs['edge_entered'])
        if attrs.get('edge_vaporized'):
            num_vehs -= float(attrs['edge_vaporized'])
        if speed < 0:
            print("Warning: invalid speed '%s' for edge '%s' when parsing dump" % (speed, edge), file=sys.stderr)
            speed = None
        return edge, num_vehs, speed


def interpret_dump(dumpfile, intervalLength, dumpInterpretation):
    dumpReader = DumpReader(dumpfile, dumpInterpretation)
    dumpReader.updateDB(intervalLength)

    
def _getConfigEntry(section, option):
    subOption = option + "." + options.region
    if config.has_option(section, subOption):
        return config.get(section, subOption)
    return config.get(section, option)

if __name__ == "__main__":
    optParser = OptionParser()
    optParser.add_option("-r", "--region", dest="region", type="choice",
                         choices=('koeln', 'muenchen', 'oberbayern', 'deutschland', 'utralab', 
                                  'stdp_test', 'mtdp_test', 'nwde', 'nrw', 'braunschweig', 'test_region',
                                  'isar', 'grohnde'),
                         default='oberbayern',
                         help="REGION to simulate [default: %default]", metavar="REGION")
    optParser.add_option("-b", "--base", default='2013-07-01 00:00',
                         help="base date (and time) to add to simulation time [default: %default]")
    optParser.add_option("-v", "--verbose", action="store_true", dest="verbose",
                         default=False, help="tell me what you are doing")
    (options, args) = optParser.parse_args()
    rootPath = os.path.normpath(os.path.join(os.path.dirname(sys.argv[0]), '..', '..'))
    configPath = os.path.join(rootPath, 'data', 'install.cfg')
    sys.path.append(os.path.join(rootPath, 'tools', 'loop_schema'))
    import loop
    setting.init(loop, configPath)
    setting.setRegion(options.region)
    for arg in args:
        dumpReader = DumpReader(arg, {"simulation_traffic": (None, None, None)})
        dumpReader.updateDB(base=datetime.datetime.strptime(options.base, "%Y-%m-%d %H:%M"))
