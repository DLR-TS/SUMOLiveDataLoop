# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    database.py
# @author  Michael Behrisch
# @date    2010-01-29
"""
Database interface for the Delphi simulation setup.
"""
from __future__ import print_function
import sys, traceback
from datetime import datetime, timedelta

import setting

try:
    import psycopg2 as pgdb
except ImportError:
    import pgdb
    print("Warning! You are using the outdated pgdb driver, please install psycopg2.", file=sys.stderr)

try:
    import cx_Oracle
    haveOracle = True
except ImportError:
    haveOracle = False
    print("Warning! Oracle client is not available.", file=sys.stderr)

OperationalError = pgdb.OperationalError

VERBOSE = False

def createDatabaseConnection(prefix="", dbPrefix=""):
    if setting.hasOption("Database", "testinput"):
        return setting.getOption("Database", "testinput")

    if not haveOracle or (setting.hasOption("Database", "postgres") and setting.getOptionBool("Database", "postgres")):
        conn = pgdb.connect(host = setting.getOption("Database", prefix + "host"),
                            user = setting.getOption("Database", prefix + "user"),
                            password = setting.getOption("Database", prefix + "passwd"),
                            database = setting.getOption("Database", dbPrefix + "db"))
    else:
        conn = cx_Oracle.connect("%s/%s@%s/%s" % (setting.getOption("Database", prefix + "user"),
                                                  setting.getOption("Database", prefix + "passwd"),
                                                  setting.getOption("Database", prefix + "host"),
                                                  setting.getOption("Database", prefix + "db")))
    if VERBOSE:
        print("connection: ", conn)
    return conn

def createOutputConnection():
    if setting.hasOption("Database", "separateOutput") and setting.getOptionBool("Database", "separateOutput"):
        return createDatabaseConnection("output", "output")
    else:
        return createDatabaseConnection()

def debug_print(command):
    if VERBOSE:
        printLength = 1000
        print("length: ", len(command))
        print(command[:printLength], (' ...' if len(command) > printLength else ''))

def execSQL(conn, commands, doCommit=False, manySet=None, fetchId=False,
            returnDescription=False, search_path=None, returnRowcount=False):
    """Executes the given SQL commands for the given database connection.
    Returns all resulting rows for reading statements, None for writing.
    If doCommit is True a commit is issued."""
    pre = datetime.now()
    if not isinstance(commands, list):
        commands = [commands]
    try:
        cursor = conn.cursor()
        try:
            if search_path is None and setting.dbSchema is not None:
                search_path = setting.dbSchema.SEARCH_PATH
            if search_path:
                debug_print(search_path)
                cursor.execute(search_path)
            for command in commands:
                debug_print(command)
                if manySet is not None:
                    debug_print(str(manySet[:10]))
                    cursor.executemany(command, manySet)
                else:
                    cursor.execute(command)
        except OperationalError as message:
            if message[0] in [1205, 1213]: # retry on lock wait timeout and deadlock
                if message[0] == 1205:
                    conn.rollback()
                print("Warning! %s" % message[1], file=sys.stderr)
                print(" Retrying once.", file=sys.stderr)
                for command in commands:
                    debug_print(command)
                    if manySet != None:
                        cursor.executemany(command, manySet)
                    else:
                        cursor.execute(command)
            else:
                raise
        if manySet is not None or doCommit or command.upper().startswith("DELETE"):
            rows = None
        else:
            rows = cursor.fetchall()
        if doCommit:
            if fetchId and manySet is None:
                if callable(fetchId): # workaround for oracle
                    rows = fetchId()
                else:
                    rows = cursor.fetchone()
            conn.commit()
        description = cursor.description
        rowcount = cursor.rowcount
        cursor.close()
    except:
        try:
            conn.close()
        except OperationalError as message:
            print("Warning! %s" % message, file=sys.stderr)
        print("Error on query '%s'" % commands, file=sys.stderr)
        raise
    setting.databaseTime += datetime.now() - pre
    if returnDescription:
        return rows, description
    elif returnRowcount:
        return rows, rowcount
    else:
        return rows

def as_time(db_val):
    """return db_val as datetime"""
    if isinstance(db_val, str): # old pg driver
        return datetime.strptime(db_val,'%Y-%m-%d %H:%M:%S')
    else:
        return db_val

def as_interval(db_val):
    """return db_val as timedelta"""
    if isinstance(db_val, str): # old pg driver
        fields = db_val.split()
        if len(fields) > 1: # days >= 1
            days, dummy, hms = fields
        else: # days < 1
            days, hms = 0, fields[0]
        hours, minutes, seconds = hms.split(':')
        return timedelta(days=int(days), hours=int(hours), 
                minutes=int(minutes), seconds=float(seconds))
    else:
        return db_val

def as_lon_lat(db_val):
    return db_val[6:-1].split()
