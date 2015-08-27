#!/usr/bin/env python

import sys
import os

command = "node src/server.js"
print "# Running command: %s" % (command)
sys.exit(os.system(command))