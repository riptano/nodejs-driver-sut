#!/usr/bin/env python

import sys
import os
import argparse
import shutil

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

contact_points = "127.0.0.1"
graphite_host = "127.0.0.1"
queries_per_http = 100
limit_per_http= 50
connections_per_host = 8

if args.configuration_file is not None:
    import ConfigParser
    config = ConfigParser.RawConfigParser({"cassandra_contact_points": contact_points,
                                           "metrics_export_graphite_host": graphite_host,
                                           "cql_queries_per_http_request": queries_per_http,
                                           "cql_limit_per_http": limit_per_http,
                                           "connections_per_host": connections_per_host})
    config.read(args.configuration_file)
    contact_points = config.get("run", "cassandra_contact_points")
    graphite_host = config.get("run", "metrics_export_graphite_host")
    queries_per_http = config.get("run", "cql_queries_per_http_request")
    limit_per_http = config.get("run", "cql_limit_per_http")
    connections_per_host = config.get("run", "connections_per_host")

command = "node %s/src/server.js %s %s %s %s %s" % (
    current_directory, contact_points, graphite_host, queries_per_http, limit_per_http, connections_per_host)
print "# Running command: %s" % (command)
sys.exit(os.system(command))