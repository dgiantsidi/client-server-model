#! /usr/bin/env python3

import queue
import subprocess
import threading
import argparse
import sys
import os
from time import sleep
from typing import List

PORT=31850

parameters = ["--hostname", "localhost",
"--port", str(PORT),
"--n_messages", "12000",
"--c_threads", "10",
"--s_threads", "4",
"--one_run", "true",
]

def run_client(binary_dir: str, source_dir: str) -> int:
  """
  Runs the client binary with the given binary_dir.
  """
  sleep(0.5)
#  return subprocess.Popen([binary_dir + "/clt", "10", "localhost", str(PORT), "20000"], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
  cmd = [binary_dir + "/clt"]
  cmd.extend(parameters)
  return subprocess.Popen(cmd)

def run_server(binary_dir: str, source_dir: str) -> int:
  """
  Runs the server binary with the given binary_dir.
  """
#  return subprocess.Popen([binary_dir + "/svr", "4", str(PORT), "localhost", "1", "10"], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
  cmd = [binary_dir + "/svr"]
  cmd.extend(parameters)
  return subprocess.Popen(cmd)


def complete(process, q):
  q.put(process.wait())


class logger:
  def __init__(self, name) -> None:
    self.name = name

  def __call__(self, stdout):
    for line in stdout:
      print(self.name + ": " + line.decode("utf-8").strip(), file=sys.stderr)

def output(log, process):
  threading.Thread(target=log, args=(process.stdout,)).start()

def main(argv: List[str]):
  q = queue.Queue()
  parser = argparse.ArgumentParser(description='Runs a small test of the server and client.')
  parser.add_argument('binary_dir', type=str, help='The directory containing the server and client binaries.')
  parser.add_argument('source_dir', type=str, help='The directory containing the source code.')
  args = parser.parse_args(argv[1:])
  parameters.extend(["--trace", args.source_dir + "/source/workload_traces/12K_traces.txt"])

  server = run_server(args.binary_dir, args.source_dir)
#  svr_log = logger("svr")
#  output(svr_log, server)
  client = run_client(args.binary_dir, args.source_dir)
#  clt_log = logger("clt")
#  output(clt_log, client)
  threads = [threading.Thread(target=complete, args=(p, q)) for p in [server, client]]
#  threads = [threading.Thread(target=complete, args=(p, q)) for p in [client]]
  for t in threads:
    t.start()
  res1 = q.get()
  server.terminate()
  client.terminate()
  try:
    res2 = q.get(timeout=1)
  except queue.Empty:
    print("Timeout")
    server.kill()
    client.kill()
    return 1
  if res1 == 0:
    return 0
  return res1
  



if __name__ == "__main__":
  sys.exit(main(sys.argv))
