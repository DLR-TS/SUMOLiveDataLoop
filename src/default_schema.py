#!/usr/bin/env python
# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    default_schema.py
# @author  Yun-Pang Floetteroed(yun-pang.floetteroed@dlr.de)
# @author  Michael Behrisch(michael.behrisch@dlr.de)
# @date    2024-11-15

"""
Schema file for use with sumo_ldl
"""
import os, sys

from sqlalchemy import Integer, DateTime, ForeignKey, String, BigInteger, SmallInteger, TypeDecorator, inspect
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from sumo_ldl.tools import Table, noneToNull, SAFE_DIV, reversedMap
from sumo_ldl.main import main
from sumo_ldl import database

INFRA_SCHEMA = "tdp_brunswick_infra"
DATA_SCHEMA = "tdp_brunswick_data"

class Base(DeclarativeBase):
    pass

class InductionLoop(Base):
    __tablename__ = INFRA_SCHEMA + ".induction_loop"

class InductionLoopGroup(Base):
    __tablename__ = INFRA_SCHEMA + ".induction_loop_group"

class InductionLoopData(Base):
    __tablename__ = DATA_SCHEMA + ".induction_loop_data"

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

    simulation_emission = Table('tdp_leipzig_sumo2024q2.sumo_emission_interval',
            traffic_id='interval_id',
            traffic_time='interval_end_time')

    prediction_emission = Table('tdp_leipzig_sumo2024q2.sumo_emission_pred_interval',
            traffic_id='interval_id',
            traffic_time='interval_end_time')
class CorrectDetector:
    corrected_columns = ','.join([
        Tables.corrected_loop_data.original_data_id,
        'correction_type',
        'induction_loop_id', 
        'data_time', 'q_pkw', 'q_lkw', 'v_pkw', 'v_lkw', 'quality'])

    enableVisual = False
    @staticmethod
    def get_measurements_for_interval(conn, correctStart, correctEnd, updateInterval, detector_filter=""):

        # get all detector values which are of the given type and in time interval
    #   selectPara = "d.q_kfz, d.q_lkw, d.v_pkw, d.v_lkw, street_type"
        selectPara = "d.%s, d.%s, d.%s, d.%s, %s" %(
                    Tables.induction_loop_data.q_kfz,
                    Tables.induction_loop_data.q_lkw,
                    Tables.induction_loop_data.v_pkw,
                    Tables.induction_loop_data.v_lkw,
                    Tables.induction_loop_group.street_type)
        selectPara = "d.q_car/12 + d.q_truck/12, d.q_truck/12, d.s_car, d.s_truck, NULL"

        command = """SELECT d.%s, d.%s, d.data_time, %s 
            FROM %s d, %s i, %s g WHERE d.%s = i.%s 
            AND i.%s = g.%s
            AND i.%s = %s
            AND d.data_time >= %s '%s' 
            AND d.data_time < %s '%s'
            %s
            ORDER BY d.%s """ % (
                    Tables.induction_loop_data.induction_loop_data_id,
                    Tables.induction_loop_data.induction_loop_id,
                    selectPara,
                    Tables.induction_loop_data,
                    Tables.induction_loop,
                    Tables.induction_loop_group,
                    Tables.induction_loop_data.induction_loop_id,
                    Tables.induction_loop.induction_loop_id,
                    Tables.induction_loop.induction_loop_group_id,
                    Tables.induction_loop_group.induction_loop_group_id,
                    Tables.induction_loop.loop_interval,
                    updateInterval.seconds,
                    AggregateData.getTimeStampLabel(), correctStart,
                    AggregateData.getTimeStampLabel(), correctEnd,
                    detector_filter,
                    Tables.induction_loop_data.induction_loop_data_id)
        return database.execSQL(conn, command)
class EvalDetector:
    # speed_value_in_db * kmhMultiplier = speed_in_kmh
    kmhMultiplier = 1.0 # 1: speeds in DB are in km/h; 3.6: speeds in DB are in m/s


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

    TYPE2EMISSIONSCHEME = {
            'simulation': (Tables.simulation_emission, 
                'tdp_leipzig_sumo2024q2.sumo_emission_history', 'nox_normed, co_normed, pmx_normed, hc_normed, co2_normed'),
            'prediction': (Tables.prediction_emission, 
                'tdp_leipzig_sumo2024q2.sumo_emission_pred_history', 'nox_normed, co_normed, pmx_normed, hc_normed, co2_normed'),
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
    def getIntervalID(conn, typeName, intervalEnd, intervalLength, emissionTable = False):
        """retrieve id of an existing interval or insert a new one and return it's id"""
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

    @staticmethod
    def insertData(conn, typeName, values):
        if len(values) == 0:
            return
        intervalTable, dataTable, q_column, v_column = AggregateData.getSchema(typeName)
        database.execSQL(conn, "DELETE FROM %s WHERE %s = %s"  % (dataTable, intervalTable.traffic_id, values[0][0]))
        command = "INSERT INTO %s(%s, edge_id, %s, %s, quality) VALUES(%s)" % (
                  dataTable, intervalTable.traffic_id, q_column, v_column,
                  ", ".join(["%s"] * len(values[0])))
        database.execSQL(conn, [command], doCommit=True, manySet=values)

    @staticmethod
    def getEmissionSchema(typeName):
        return AggregateData.TYPE2EMISSIONSCHEME[typeName]

    @staticmethod
    def insertEmissionData(conn, typeName, values):
        if len(values) == 0:
            return
        intervalTable, dataTable, emissionColumns = AggregateData.getEmissionSchema(typeName)
        database.execSQL(conn, "DELETE FROM %s WHERE %s = %s"  % (dataTable, intervalTable.traffic_id, values[0][0]))
        command = "INSERT INTO %s(%s, edge_id, %s, quality) VALUES(%s)" % (
                  dataTable, intervalTable.traffic_id, emissionColumns,
                  ", ".join(["%s"] * len(values[0])))
        database.execSQL(conn, [command], doCommit=True, manySet=values)

    @staticmethod
    def getComparisonData(conn, typeName, time):
        intervalTable, dataTable, q_column, v_column = AggregateData.getSchema(typeName)
        edge_column = "edge_id"
        rows = database.execSQL(conn, """
            SELECT %s, %s, %s
            FROM %s t, %s a
            WHERE t.%s = a.%s AND %s = %s '%s' %s""" % (
                      edge_column, q_column, v_column,
                      intervalTable, dataTable,
                      intervalTable.traffic_id,
                      Tables.traffic.traffic_id,
                      intervalTable.traffic_time,
                      AggregateData.getTimeStampLabel(),
                      time,
                      Extrapolation.getTypePredicate(typeName)))
        return rows

    @staticmethod
    def getTimeStampLabel():
        return "TIMESTAMP"
        
    
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
        """return rows of [edge_id, time, interval_length, flow_per_hour, speed_km_per_h, quality, type]"""
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
                        Tables.traffic.traffic_id,
                        Tables.traffic.traffic_id,
                        Tables.traffic.traffic_time, begin, 
                        Tables.traffic.traffic_time, end, 
                        qualityThreshold,
                        Tables.traffic.traffic_time)
            #print query
            rows = database.execSQL(conn, query)
            result += [(reverseEdgeMap.get(e), t, intervalLength, q, v, qual, type) for e,t,q,v,qual in rows]
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
