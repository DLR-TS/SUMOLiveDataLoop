import sys, os
import subprocess

leipzig_dir = r"C:\Wang\repos\simo\trunk\projects\Leipzig\sumo_online_test"
logfile = os.path.join(os.getcwd(), "ldl_log.txt")


subprocess.call([sys.executable, "leipzig_schema.py", "-c", "leipzig_online.cfg", "-t", "detector", "--test", "--log", logfile], cwd=leipzig_dir)
