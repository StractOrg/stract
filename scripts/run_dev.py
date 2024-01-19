#!.venv/bin/python3
import argparse
import subprocess
import os

os.environ["LIBTORCH"] = "libtorch"
os.environ["LD_LIBRARY_PATH"] = "libtorch/lib"
os.environ["DYLD_LIBRARY_PATH"] = "libtorch/lib"

parser = argparse.ArgumentParser()

parser.add_argument("--release", action="store_true")

args = parser.parse_args()

if args.release:
    os.environ["STRACT_CARGO_ARGS"] = "--release"

processes = []

processes.append(subprocess.Popen(["just", "dev-api"]))
processes.append(subprocess.Popen(["just", "dev-search-server"]))
processes.append(subprocess.Popen(["just", "dev-webgraph"]))
processes.append(subprocess.Popen(["just", "dev-frontend"]))
processes.append(subprocess.Popen(["just", "dev-llm"]))

# kill processes on ctrl-c
import time

while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        for p in processes:
            p.kill()
        break
