#!/usr/bin/env python

import sys
import os
import argparse
import shutil


def exec_command(command):
    print "# Executing command: %s" % (command)
    ret = os.system(command)
    if ret != 0:
        sys.exit(1)

def configuration_file(value):
    if os.path.isfile(value):
        return value
    else:
        raise argparse.ArgumentTypeError("%s does not exist" % value)


parser = argparse.ArgumentParser(description="Node.js Driver SUT build",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--configuration-file", required=False, type=configuration_file,
                    help="The configuration file that should be used")
args = parser.parse_args()

current_directory = os.path.dirname(os.path.realpath(__file__))
print "# Configuration file: %s" % (args.configuration_file)
print "# Current directory: %s" % (current_directory)

branch = "master"
if args.configuration_file is not None:
    import ConfigParser
    config = ConfigParser.RawConfigParser({"driver_branch": branch})
    config.read(args.configuration_file)
    branch = config.get("build", "driver_branch")

exec_command("npm install datastax/nodejs-driver#%s --prefix %s" % (branch, current_directory))
exec_command("npm install --prefix %s" % (current_directory))