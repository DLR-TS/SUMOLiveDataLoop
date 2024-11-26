#!/usr/bin/env python
# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    default_schema.py
# @author  Jakob Erdmann
# @author  Michael Behrisch
# @date    2013-04-18

"""
Schema file for use with sumo_ldl
"""
import os, sys
from sumo_ldl.tools import Table, noneToNull, SAFE_DIV, reversedMap
from sumo_ldl.main import main
from sumo_ldl import database

class Tables:
    # only tables and columns which differ between schemas are included

    induction_loop = Table('idb_sensor_induction_loop',
            loop_interval='aggregation_interval_q',
            loop_interval_v='aggregation_interval_v')

    induction_loop_group = Table('idb_sensor_induction_loop_group',
        position='distance_to_edge_origin',
        geom_wgs84='position')

    corrected_loop_data = Table('tdp_induction_loop_data_corrected',
            original_data_id='induction_loop_data_raw_id')

    induction_loop_data = Table('tdp_induction_loop_data_raw',
            database_time='db_entry_time',
            induction_loop_data_id='induction_loop_data_raw_id')

    operating_status = Table('tdp_induction_loop_operating_status')

    process_step = None

    process = None

    traffic = Table('AMBIGUOUS_DO_NOT_USE',
        traffic_id='interval_id',
        traffic_time='interval_end_time')

    loop_traffic = Table('tdp_induction_loop_aggregation_interval',
            traffic_id='interval_id',
            traffic_time='interval_end_time')

    fcd_traffic = Table('tdp_fcd_aggregation_interval',
            traffic_id='interval_id',
            traffic_time='interval_end_time')

    fusion_traffic = Table('tdp_fusion_data_aggregation_interval',
            traffic_id='interval_id',
            traffic_time='interval_end_time')

    extrapolation_traffic = Table('tdp_extrapolation_interval',
            traffic_id='interval_id',
            traffic_time='interval_end_time')

    simulation_traffic = Table('tdp_simulation_interval',
            traffic_id='interval_id',
            traffic_time='interval_end_time')

    prediction_traffic = Table('tdp_simulation_prediction_interval',
            traffic_id='interval_id',
            traffic_time='interval_end_time')


    edge = Table('idb_net_edge',
            navteq_id='sumo_id')

    floating_car_data = Table('tdp_fcd_on_edges',
        v_kfz='travel_speed',
        data_time='local_time',
        coverage='cover')

    traffic_signal = Table('idb_net_traffic_signal',
        edge_id='edge_id_in')

    edge_connection = Table('idb_net_edge_connection',
            in_edge='edge_id_in',
            out_edge='edge_id_out')

class CorrectDetector:
    corrected_columns = ','.join([
        Tables.corrected_loop_data.original_data_id,
        'correction_type',
        'induction_loop_id', 
        'data_time', 'q_pkw', 'q_lkw', 'v_pkw', 'v_lkw', 'quality'])

    enableVisual = False


class EvalDetector:
    # speed_value_in_db * kmhMultiplier = speed_in_kmh
    kmhMultiplier = 1.0 # dbSpeeds are in km/h already


    @staticmethod
    def toValues(instance, date, qPKW, qLKW, vPKW, vLKW, quality):
        Data = instance.__class__
        if instance.origID == Data.NO_ORIG_DATA:
            origID = 'Null'
            correctionType = 'missing' 
        elif instance.origID == Data.FORECAST_DATA:
            origID = 'Null'
            correctionType = 'forecast' 
        else:
            origID = int(instance.origID)
            correctionType = 'existing'
        return "(%s,'%s',%s,'%s',%s,%s,%s,%s,%s)" % (
                origID, 
                correctionType,
                int(instance.detID), 
                date, qPKW, qLKW, vPKW, vLKW, quality)


class Loop:
    region_choices = ('braunschweig',)
    default_type = 'simulation'
    default_config = '../default.cfg'


class AggregateData:
    NEED_EDGE_MAP = False # whether simulation edge ids differ from database edge ids

    TYPE2SCHEME = {
            'loop': (Tables.loop_traffic, 
                'tdp_induction_loop_aggregated_history', 'q', 'v'),
            'fcd': (Tables.fcd_traffic, 
                'tdp_fcd_aggregated_history', 'count', 'travel_speed'),
            'fusion': (Tables.fusion_traffic, 
                'tdp_fusion_data_aggregated_history', 'q', 'v'),
            'extrapolation': (Tables.extrapolation_traffic, 
                'tdp_extrapolation_history', 'q', 'v'),
            'simulation': (Tables.simulation_traffic, 
                'tdp_simulation_history', 'q', 'v'),
            'prediction': (Tables.prediction_traffic, 
                'tdp_simulation_prediction_history', 'q', 'v'),
            }

    @staticmethod
    def getSchema(typeName):
        return AggregateData.TYPE2SCHEME[typeName]

    @staticmethod
    def update_description(dataTable, typeName):
        return dataTable


    @staticmethod
    def getSimulationEdgeMap(conn):
        """map from simulation edge id to database edge id"""
        #return pickle.load(open("simulationEdgeMap.pkl", 'rb'))

        # network compatible with db
        result = {}
        rows = database.execSQL(conn, "SELECT %s, edge_id FROM %s" % (
            Tables.edge.navteq_id,
            Tables.edge))
        for navteqID, dbID in rows:
            result[str(navteqID)] = dbID
        return result


    @staticmethod
    def getIntervalID(conn, typeName, intervalEnd, intervalLength):
        """retrieve id of an existing interval or insert a new one an return it's id"""
        intervalTable = AggregateData.getSchema(typeName)[0]
        indexRow = database.execSQL(conn, """
                SELECT %s FROM %s WHERE %s='%s'""" % (
            Tables.traffic.traffic_id,
            intervalTable, 
            Tables.traffic.traffic_time,
            intervalEnd))
        if indexRow:
            return indexRow[0][0]
        else:       
            trafficInsert = "INSERT INTO %s(%s) VALUES ('%s') RETURNING %s" % (
                    intervalTable, 
                    Tables.traffic.traffic_time,
                    intervalEnd,
                    Tables.traffic.traffic_id)
            row = database.execSQL(conn, [trafficInsert], doCommit=True, fetchId=True)
            return row[0] # only a single row due to fetchone()


class Extrapolation:
    @staticmethod
    def getTypePredicate(typeName):
        return ""


class Detector:
    detconn_default = ""

    @staticmethod
    def insert_induction_loop_query(groupID, detector):
        return """INSERT INTO %s(induction_loop_group_id, data_id, %s, %s, lane_no, description, vendor, direction_of_traffic, delivery_interval)
                  VALUES(%s, '%s', %s, %s, %s, '%s', '%s', '%s', %s)""" % (
                          Tables.induction_loop,
                          Tables.induction_loop.loop_interval,
                          Tables.induction_loop.loop_interval_v,
                          groupID, detector.id, detector.interval, detector.interval, detector.lane,
                          detector.description, 
                          detector.vendor,
                          detector.direction_of_traffic,
                          detector.interval)

    @staticmethod
    def insert_induction_loop_group_query(edge, group):
        geometryString = "geometryFromText('POINT(%s %s)',4326)" % (group.longitude, group.latitude)
        return """INSERT INTO %s(edge_id, %s, description, street_type, %s,
                  next_location_code, next_location_name, loop_type,
                  road_km, road_name, data_source, 
                  number_of_loops)
                  VALUES(%s, %s, '%s', '%s', %s, %s, '%s', '%s', %s, '%s', '%s', %s)
                  RETURNING induction_loop_group_id""" % (
                          Tables.induction_loop_group,
                          Tables.induction_loop_group.position,
                          Tables.induction_loop_group.geom_wgs84,
                          edge, group.pos, group.description,
                          group.streetType, geometryString,
                          noneToNull(group.next_location_code),
                          noneToNull(group.next_location_name),
                          noneToNull(group.loop_type),
                          noneToNull(group.road_km),
                          noneToNull(group.road_name),
                          noneToNull(group.data_source),
                          len(group.detectors))

class GenerateSimulationInput:

    @staticmethod
    def getTypedTrafficValues(conn, types, begin, end, qualityThreshold, intervalLength, timeline):
        """return rows of [edge_id, time, interval_length, flow, speed_m_per_s, quality, type]"""
        if timeline: 
            raise Exception("timeline not support for dbSchema '%s'" % __file__)
        result = []
        reverseEdgeMap = reversedMap(AggregateData.getSimulationEdgeMap(conn))
        for type in types:
            intervalTable, dataTable, q_column, v_column = AggregateData.getSchema(type)
            query = """
                SELECT edge_id, %s, %s, %s, a.quality
                FROM %s t, %s a WHERE true
                AND a.%s = t.%s 
                AND %s > '%s' 
                AND %s <= '%s' 
                AND a.quality > %s
                ORDER BY %s, edge_id
                """ % (Tables.traffic.traffic_time,
                        q_column, v_column,
                        intervalTable, dataTable,
                        Tables.traffic.traffic_id, Tables.traffic.traffic_id,
                        Tables.traffic.traffic_time, begin, 
                        Tables.traffic.traffic_time, end, 
                        qualityThreshold,
                        Tables.traffic.traffic_time)
            #print query
            rows = database.execSQL(conn, query)
            result += [(reverseEdgeMap.get(e), t, intervalLength, q, SAFE_DIV(v, 3.6), qual, type) for e,t,q,v,qual in rows]
        return result


    @staticmethod
    def getRestrictionQuery(intervalBegin):
        # XXX correspondence between e.allowed (13bits) and e.vehicle_type (12bits)?
        # XXX equivalent of intervalBegin < t.time_end?
        # XXX number_of_lanes is always NULL
        return """SELECT t.edge_id, t.edge_id, validity_period
        FROM idb_net_edge_restriction t, %s e
        WHERE t.edge_id = e.edge_id 
        --AND e.vehicle_type & B'101100000000' != B'0' 
        AND t.number_of_lanes = 0""" % (
                      Tables.edge,)


class BuildNetwork:

    @staticmethod
    def getConnectionsQuery(dbIDs):
        return "SELECT edge_id_out, edge_id_in, lane_no_out, lane_no_in FROM idb_net_lane_connection"

    @staticmethod
    def getEdgeAndNodesQuery():
        return "SELECT edge_id, source_node_id, target_node_id FROM idb_net_edge"

    @staticmethod
    def getEdgeQuery(streetType):
        return """SELECT edge_id, length, number_of_lanes, speed_limit,
                         vehicle_type, ST_ASTEXT(geometry), edge_id, functional_road_class 
                  FROM idb_net_edge 
                  WHERE functional_road_class <= %s""" % streetType

    @staticmethod
    def getNodeQuery():
        return "SELECT node_id, ST_ASTEXT(position), node_id FROM idb_net_node"

    @staticmethod
    def getGeometry(linestring):
        #example: LINESTRING(3.74508 19.88115,3.72161 19.86177,3.70484)
        geometry = linestring[11:-1].split(",")
        return [map(float, p.split()) for p in geometry]


if __name__ == "__main__":
    #print 'dbSchema set to %s' % AIMSchema.name
    loopDir = os.path.dirname(sys.argv[0])
    main(sys.modules[__name__], loopDir)
