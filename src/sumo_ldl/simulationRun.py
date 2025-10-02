# SUMO Live Data Loop
# Copyright (C) 2007-2024 German Aerospace Center (DLR) and others.
# This program and the accompanying materials are made available under the
# terms of the Eclipse Public License 2.0 which is available at
# https://www.eclipse.org/legal/epl-2.0/
# SPDX-License-Identifier: EPL-2.0

# @file    simulationRun.py
# @author  Michael Behrisch
# @date    2007-07-18
"""
Executes one simulation run including input generation and output parsing.
Usually it is called from loop.py.
"""
import os, sys, shutil, glob, pickle, subprocess, re
from datetime import datetime, timedelta

from . import generateSimulationInput, generateViewerInput, routeDistributions, aggregateData, generateEmissionOutput
from . import setting, tools, database
from .setting import hasOption, getLoopOption, getOptionInt, getLoopOptionBool, getLoopOptionMinutes,\
                    getOSDependentLoopOptionPath, getLoopOptionPathList, getDetectorOptionBool
from .step import systemStep, pythonStep

STATE_FILE = "state.xml.gz"

def buildDirs(root, currTime, timeformat, repeat, simBegin):
    checkDir = os.path.join(root, 'check', currTime.strftime(timeformat))
    simInputDir = os.path.join(root, 'sim_inputs', currTime.strftime(timeformat))
    simOutputDir = os.path.join(root, 'sim_outputs', currTime.strftime(timeformat))
    lastStateDir = os.path.join(root, 'sim_outputs', (currTime-repeat).strftime(timeformat))
    statefile = os.path.join(lastStateDir, STATE_FILE)
    for outdir in [checkDir, simInputDir, simOutputDir]:
        if not os.path.exists(outdir):
            os.makedirs(outdir)
    return checkDir, simInputDir, simOutputDir, statefile

def onRemovalError(func, path, exc_info):
    print("Warning! Could not remove %s." % path, file=sys.stderr)
    print(exc_info[1], file=sys.stderr)

def diskAvailable(path):
    if os.name == "posix":
        stats = os.statvfs(path)
        return stats.f_bsize * stats.f_bavail
    if os.name == "nt":
        output = subprocess.Popen(["dir", path], stdout=subprocess.PIPE, shell=True).communicate()[0]
        bytes = re.findall('([0-9.,]*) [Bb]ytes', output.splitlines()[-1])[0]
        return int(re.sub("[,.]", "", bytes))

def copyBackupClean(root, currTime, simOutputDir):
    mdate = currTime.strftime("_%Y%m%d_%H%M00")
    for targetDir in getLoopOptionPathList("viewerData"):
        if not os.path.exists(targetDir):
            os.makedirs(targetDir)
        print("locking", targetDir, 'TEXTTEST_IGNORE')
        lock = open(os.path.join(targetDir, "lock.txt"), 'w')
        print(currTime, 'TEXTTEST_IGNORE', file=lock)
        lock.close()
        for f in ["simulation", "prediction", "compare"]:
            filepath = os.path.join(simOutputDir, f + ".txt")
            if os.path.exists(filepath):
                print("copy", f, targetDir, 'TEXTTEST_IGNORE')
                shutil.copyfile(filepath, os.path.join(targetDir, f + mdate + ".txt"))
        print("unlocking", targetDir, 'TEXTTEST_IGNORE')
        os.remove(os.path.join(targetDir, "lock.txt"))
    # delete all files beyond the specified age
    for deldir in ["sim_outputs", "check", "sim_inputs"] + getLoopOptionPathList("viewerData"):
        for f in sorted(glob.glob(os.path.join(root, deldir, "*"))):
            if datetime.fromtimestamp(os.path.getmtime(f)) < setting.startTime - getLoopOptionMinutes("deleteafter"):
                shutil.rmtree(f, onerror=onRemovalError)
    # delete big files to maintain minimum free disk space
    MIN_FREE_BYTES = 10 * 2**30 # 10GB
    for f in sorted(glob.glob(os.path.join(root, "sim_outputs", "*", STATE_FILE))):
        if diskAvailable(f) < MIN_FREE_BYTES:
            try:
                os.remove(f)
            except:
                print("Warning! Could not remove %s." % f, file=sys.stderr)
                print(sys.exc_info(), file=sys.stderr)
        else:
            break


def prepare_dump_helper(type, i, aggregation, finalTime, simbegSec, simOutputDir, 
                        fd, dumpfile, dumpInterpretation,
                        emissionInterpretation=None, emissionfile=None, emissionNormed=True,
                        withInternal=False):
    """writes to dumpAdd fd and adds entry to dumpInterpretation"""
    end = finalTime - i * aggregation
    beg = end - aggregation
    endSec = tools.daySecond(end, simbegSec)
    begSec = tools.daySecond(beg, simbegSec)
    id = '%s%s' % (type, i)
    file = (os.path.join(simOutputDir, '%s.txt' % type) if i == 0 else None)
    print('    <edgeData id="%s" begin="%s" end="%s" file="%s" excludeEmpty="true" withInternal="%s" writeAttributes="speed departed entered vaporized"/>' % (   # only for huainan todo: check with the simulation performance if data from the internal links should be used.
            id, begSec, endSec, dumpfile, withInternal), file=fd)
    dumpInterpretation[id] = (end, type, file)
    if emissionfile:
        file = (os.path.join(simOutputDir, 'emission_%s.txt' % type) if i == 0 else None)
        if emissionNormed:
            attributes = ["%s_normed" % e for e in ('CO', 'CO2', 'HC', 'PMx', 'NOx', 'fuel', 'electricity')]
        else:
            attributes = ["%s_abs" % e for e in ('CO', 'CO2', 'HC', 'PMx', 'NOx', 'fuel', 'electricity')]
        print('    <edgeData id="%s" begin="%s" end="%s" file="%s" type="emissions" excludeEmpty="true" withInternal="%s" writeAttributes="%s"/>' % (
                id, begSec, endSec, emissionfile, withInternal, " ".join(attributes)), file=fd)
        emissionInterpretation[id] = (end, type, file)
    

def prepare_dump(simInputDir, simOutputDir, simbegSec, startTime, simEnd, aggregation, repeat, forecast, emissionOutput, withInternal):
    """prepares the edgeData output and the necessary information for parsing
    this output. There should be enough dumps to cover all aggregation intervals"""
    dumpInterpretation = {} # edgeDataID -> (intervalEnd, traffic_type, fileName|None)
    numDumpsSimulation, restSeconds = divmod(repeat.seconds, aggregation.seconds)
    numDumpsPrediction, restSeconds = divmod(forecast.seconds, aggregation.seconds)
    if restSeconds > 0:
        print("Warning: Repeat is not a multiple of aggregation.  Aggregated_traffic and simulation_traffic will be out of sync.")
    dumpAdd =  'dump.add.xml'
    dumpfile = os.path.abspath(os.path.join(simOutputDir, 'dump_%s_%s.csv.gz' % (
        startTime.strftime("%H-%M"), aggregation.seconds))) 
    emissionfile = None
    if emissionOutput:
        emissionInterpretation = {}   # edgeDataID -> (intervalEnd, traffic_type, fileName|None)
        emissionfile = os.path.abspath(os.path.join(simOutputDir, 'emission_%s_%s.csv.gz' % (
            startTime.strftime("%H-%M"), aggregation.seconds))) 
    with open(os.path.join(simInputDir, dumpAdd), "w") as fd:
        print("<a>", file=fd)
        for i in range(numDumpsSimulation):
            prepare_dump_helper('simulation', i, aggregation, startTime, simbegSec, simOutputDir, fd, dumpfile, dumpInterpretation, emissionInterpretation, emissionfile, True, withInternal)
        for i in range(numDumpsPrediction):
            prepare_dump_helper('prediction', i, aggregation, simEnd, simbegSec, simOutputDir, fd, dumpfile, dumpInterpretation, emissionInterpretation, emissionfile, True, withInternal)
        print("</a>", file=fd)
    return dumpAdd, dumpfile, dumpInterpretation, emissionfile, emissionInterpretation

def main(doStartEmpty, beginNewDay, loopDir, options):
    scenario = options.scenario
    if options.clean and doStartEmpty:
        if getLoopOptionBool("emissionOutput"):
            pythonStep("Cleaning database",
                   aggregateData.cleanUp, (None, ["simulation", "prediction"], True))
        pythonStep("Cleaning database",
                   aggregateData.cleanUp, (None, ["simulation", "prediction"]))
    """Do everything that has to be done."""
    region = getLoopOption("region")
    root = os.path.abspath(os.path.join(loopDir, region))
    repeat = getLoopOptionMinutes("repeat")
    aggregation = getLoopOptionMinutes("aggregate")
    if doStartEmpty:
        if getLoopOptionMinutes("prefirst") < getLoopOptionMinutes("overlap"):
            print("Warning! The first simulation run should have a larger advance.")
        simBegin = setting.startTime - getLoopOptionMinutes("prefirst")
    else:
        simBegin = setting.startTime - getLoopOptionMinutes("overlap")
    saveStateTime = setting.startTime - getLoopOptionMinutes("overlap") + repeat
    forecastStart = setting.startTime
    simEnd = forecastStart + getLoopOptionMinutes("forecast")
    if saveStateTime < simBegin or saveStateTime >= simEnd:
        print("Error! Either your forecast or your prefirst setting are too small for the repeat.")
        return False
    routesPrefix = getLoopOptionPathList("routesprefix")
    if len(routesPrefix) != 7:
        print("Warning! Number of route prefixes does not match number of weekdays (7).")
    if routesPrefix:
        while len(routesPrefix) < 7:
            routesPrefix.append(routesPrefix[-1])
    routeStep = getLoopOptionMinutes("routestep")
    beginTime = datetime.now()
    print("""%s
Simulating %s to %s,
 starting at %s. TEXTTEST_IGNORE
%s""" % ("-" * 77, simBegin, simEnd, beginTime, "- " * 39))

    currTimeMin = setting.startTime.strftime("%H-%M")
    simbegSec = tools.daySecond(simBegin) 
    setting.step = 1
    if scenario:
        conn = database.createDatabaseConnection()
        rows = database.execSQL(conn, "SELECT scenario_id FROM scenario WHERE scenario_name='%s'" % scenario)
        if rows:
            setting.scenarioID = rows[0][0]
        conn.close()
    resultDirs = pythonStep("Building directories", buildDirs,
                            (os.path.join(root, scenario), setting.startTime, "%Y_%m_%d_%H-%M-%S",
                             repeat, simBegin))
    if resultDirs == None:
        print("Warning! Building directories failed, retrying once.")
        resultDirs = pythonStep("Building directories", buildDirs,
                                (region, setting.startTime, "%Y_%m_%d_%H-%M-%S",
                                 repeat, simBegin, loopDir))
    if resultDirs == None:
        print("Error! Building directories failed, exiting.")
        return False
    checkDir, simInputDir, simOutputDir, statefile = resultDirs
    if not setting.edges:
        edgePkl = open(os.path.join(root, "infra", "edges.pkl"), 'rb')
        setting.edges = pickle.load(edgePkl)
        edgePkl.close()

    resultGenerateCalibrators = pythonStep("Generating calibrator input",
                      generateSimulationInput.generateCalibrators,
                      (simInputDir, simBegin, forecastStart, simEnd, simOutputDir), checkDir, currTimeMin)
    if resultGenerateCalibrators is None:
        print("Error! Generation of calibrators failed, exiting")
        return False
    else:
        adds, calibratorEdges = resultGenerateCalibrators

    if not doStartEmpty and not os.path.exists(statefile):
        print("Warning! Could not find %s, starting empty." % statefile)
        doStartEmpty = True
    if (not doStartEmpty 
            and hasOption("Loop", "clearState") 
            and getLoopOptionBool("clearState") 
            and simbegSec == 0):
        print("'clearState=true': starting empty on new day.")
        doStartEmpty = True
    routeOutput = os.path.join(simInputDir, "static.rou.xml")
    adds = [routeOutput] + adds
    #if doStartEmpty:
    adds = getLoopOptionPathList("adds") + adds

    pythonStep("Generating static route distributions",
               routeDistributions.generateStatic,
               (routeOutput, doStartEmpty, simBegin, simEnd, calibratorEdges,
                os.path.join(root, "infra")), checkDir, currTimeMin)
    if getLoopOptionBool("collectRouteInfo"):
        routeOutput = os.path.join(simInputDir, "dynamic.rou.xml")
        pythonStep("Generating dynamic route distributions",
                   routeDistributions.generateDynamic,
                   (routeOutput, doStartEmpty, simBegin, simEnd,
                    getLoopOptionMinutes("routeInterval")), checkDir, currTimeMin)
        adds.append(routeOutput)
    blocks = pythonStep("Generating blocking input",
                        generateSimulationInput.handleBlockings,
                        (simInputDir, simBegin, simEnd), checkDir, currTimeMin)
    if blocks:
        adds += blocks

    # run simulation
    emissionOutput = hasOption("Loop", "emissionOutput") and getLoopOptionBool("emissionOutput")
    withInternal = hasOption("Loop", "withInternal") and getLoopOptionBool("withInternal")
    
    dumpAdd, dumpfile, dumpInterpretation, emissionfile, emissionInterpretation = prepare_dump(simInputDir, simOutputDir,
            simbegSec, setting.startTime, simEnd, aggregation, repeat,
            getLoopOptionMinutes("forecast"), emissionOutput, withInternal)
    adds.append(dumpAdd)
    routes = []
    routeBegin = tools.roundToMinute(simBegin, routeStep, tools.ROUND_DOWN)
    routeTime = routeBegin
    dayOffset = 0
    #dayPrefix = None
    while routeTime < routeBegin + timedelta(hours=1):
        #if dayOffset != 86400:
            #dayPrefix = routesPrefix[routeTime.weekday()]
        #fileName = "%s%s.rou.xml" % (dayPrefix, tools.daySecond(routeTime)+dayOffset)
        fileName = "%s%s.rou.xml" % (routesPrefix[routeTime.weekday()], tools.daySecond(routeTime)+dayOffset)
        if os.path.exists(fileName):
            routes.append(fileName)
        routeTime += routeStep
        if tools.daySecond(routeTime) == 0:
            dayOffset = 86400

    fd = open(os.path.join(simInputDir, 'pre.sumocfg'), "w")
    print("""<configuration>
    <input>
        <net-file value="%s"/>
        <route-files value="%s"/>
        <additional-files value="%s"/>""" % (getLoopOptionPathList("net")[0],
                                             ",".join(routes), ",".join(adds)), file=fd)
    if not doStartEmpty:
        print('<load-state value="%s"/>' % statefile, file=fd)
        if simbegSec==0:
            print('<load-state.offset value="86400"/>', file=fd)
    print("""    </input>
    <output>
        <save-state.files value="%s"/>
        <save-state.times value="%s"/>
    </output>""" % (os.path.join(simOutputDir, STATE_FILE),
                    tools.daySecond(saveStateTime, simbegSec)), file=fd)

    print("""    <time>
        <begin value="%s"/>
        <end value="%s"/>
    </time>
    <processing>
        <ignore-route-errors value="true"/>
    </processing>
    <report>
        <no-step-log value="true"/>
        <verbose value="true"/>
        <xml-validation value="never"/>
    </report>
</configuration>""" % (simbegSec, tools.daySecond(simEnd, simbegSec)), file=fd)
    fd.close()
    sumoCfg = os.path.join(simInputDir, 'sumo.sumocfg')
    subprocess.call([getOSDependentLoopOptionPath("sumobinary"), '-c', fd.name, '-C', sumoCfg, '--save-configuration.relative'] + getLoopOption("sumoOptions").split())
    command = getOSDependentLoopOptionPath("sumobinary") + ' -c ' + sumoCfg
    systemStep("Performing the simulation", command, checkDir, currTimeMin)

    if os.path.exists(dumpfile):
        pythonStep("Writing simulation data to files and DB",
                   generateViewerInput.interpret_dump,
                   (dumpfile, aggregation, dumpInterpretation),
                   checkDir, currTimeMin)
        if os.path.exists(emissionfile):
            pythonStep("Writing simulated emission data to files and DB",
                       generateEmissionOutput.interpret_emission,
                       (emissionfile, aggregation, emissionInterpretation, True),
                       checkDir, currTimeMin)
        if not hasOption("Loop", "comparison") or getLoopOptionBool("comparison"):
            pythonStep("Generating comparison data",
                   aggregateData.generateComparison,
                   (os.path.join(simOutputDir, "compare.txt"),
                    setting.startTime, ["loop", "fusion",  "simulation", "prediction"]), checkDir, currTimeMin)
        pythonStep("Copying results, making backups",
                   copyBackupClean, (root, setting.startTime, simOutputDir), checkDir, currTimeMin)
        if getOptionInt("Loop", "deleteafterDB") > 0:
            if setting.startTime - setting.lastCleanup > getLoopOptionMinutes("deleteafterDB"):
                before = setting.startTime - getLoopOptionMinutes("deleteafterDB")
                pythonStep("Cleaning database",
                           aggregateData.cleanUp, (before, ["simulation", "prediction"]), checkDir, currTimeMin)
                if getLoopOptionBool("emissionOutput"):
                    pythonStep("Cleaning database",
                               aggregateData.cleanUp, (before, ["simulation", "prediction"], True), checkDir, currTimeMin)
                setting.lastCleanup = setting.startTime
    else:
        print("Warning! Could not find %s, skipping the rest of the iteration." % dumpfile)
             
    endTime = datetime.now()
    print("""Simulation of %s to %s
 ended at %s. TEXTTEST_IGNORE
Duration: %s TEXTTEST_IGNORE
%s""" % (simBegin, simEnd, endTime, endTime - beginTime, "-" * 77))
    return True
