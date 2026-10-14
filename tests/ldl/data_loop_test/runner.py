import sys
import subprocess

leipzig_dir = r"C:\Wang\repos\simo\trunk\projects\Leipzig\sumo_online_test"

subprocess.call([sys.executable, "leipzig_schema.py", "-c", "leipzig_online.cfg", "-t", "detector", "--test"], cwd=leipzig_dir)
