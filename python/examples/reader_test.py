#!/usr/bin/env python

from jlog import *

SUBSCRIBER = "voyeur"
LOGNAME = "/tmp/jtest.foo"

reader = JLogReader(LOGNAME, SUBSCRIBER, 5)

for msg in reader:
  print msg
