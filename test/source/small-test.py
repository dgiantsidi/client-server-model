#! /usr/bin/env python3

import subprocess
import threading
import argparse
import sys
import os
from time import sleep
from typing import List

PORT=31850

def run_client(binary_dir: str) -> int:
  """
  Runs the client binary with the given binary_dir.
  """
  os._exit(subprocess.call([binary_dir + "/clt", "10", "localhost", str(PORT), "20000"]))

def run_server(binary_dir: str) -> int:
  """
  Runs the server binary with the given binary_dir.
  """
  return subprocess.call([binary_dir + "/svr", "4", str(PORT), "localhost"])


def main(argv: List[str]):
  parser = argparse.ArgumentParser(description='Runs a small test of the server and client.')
  parser.add_argument('binary_dir', type=str, help='The directory containing the server and client binaries.')
  args = parser.parse_args(argv[1:])

  server_thread = threading.Thread(target=run_server, args=(args.binary_dir,))
  client_thread = threading.Thread(target=run_client, args=(args.binary_dir,))

  server_thread.start()
  sleep(0.2)
  client_thread.start()

  client_thread.join()
  server_thread.join()



if __name__ == "__main__":
  main(sys.argv)