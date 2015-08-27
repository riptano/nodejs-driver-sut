#!/usr/bin/env python

import sys
import os
import argparse


def exec_command(command):
    print "# Executing command: %s" % (command)
    ret = os.system(command)
    if ret != 0:
        sys.exit(1)

parser = argparse.ArgumentParser(description="Node.js Driver SUT", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--branch', required=False, default="master", help="Node.js driver branch to be used")
args = parser.parse_args()

current_directory = os.path.dirname(os.path.realpath(__file__))
print "# Current directory: %s" % (current_directory)
exec_command("npm install datastax/nodejs-driver#%s" % (args.branch))
exec_command("npm install")