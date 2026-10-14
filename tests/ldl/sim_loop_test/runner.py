import sys, os
import subprocess

leipzig_dir = os.environ["SUMO_LDL_LEIPZIG"]
logfile = os.path.join(os.getcwd(), "ldl_log.txt")

subprocess.call([sys.executable, "leipzig_schema.py", "-c", "leipzig_online.cfg", "-t", "simulation", "--test", "--log", logfile], cwd=leipzig_dir)
