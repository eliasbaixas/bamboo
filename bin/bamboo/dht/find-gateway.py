#!/usr/bin/python -u
#
# Copyright (c) 2005 Regents of the University of California.
# All rights reserved.
#
# See the file LICENSE included in this distribution for details.
#
# Simple script to output the fastest opendht servers
# Input: number of servers to ping (optional 1st cmd line arg)
#        file of opendht servers (optional 2nd cmd line arg)
# Output: fastest servers, in order, for up to 5 seconds.
#
# - Barath Raghavan, 07/06/2005
#
# $Id: find-gateway.py,v 1.2 2005/08/18 18:49:40 srhea Exp $

import threading
import sys
import os
import commands
import time
import socket
import random
import urllib

class SpeedTest(threading.Thread):
    def __init__(self, servername, serverip):
        threading.Thread.__init__(self)
        self.servername = servername
        self.serverip = serverip
    def run(self):
        try: 
            for x in range(0, 3):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self.serverip, 5851))
                s.close
            print self.servername
        except:
            pass

# populate server list
if (len(sys.argv) < 3):
    u = urllib.urlopen('http://opendht.org/servers.txt')
    # throw away the first line
    time = u.readline()
    # get the server list
    servers = []
    while 1:
        line = u.readline()
        if not line: break
        linet = line.split()
        servers.append(linet[2])
else:
    serverfile = open(sys.argv[2], 'r')
    servers = serverfile.read().split()

# number of servers to ping.  by default, 32
if (len(sys.argv) < 2):
    numservers = 32
else:
    numservers = int(sys.argv[1])
    if numservers > len(servers):
        numservers = len(servers)

# randomize the server list
random.shuffle(servers)

# now test the servers
serverthreads = []

for x in range(0, numservers):
    try:
        serverthreads.append(SpeedTest(servers[x], socket.gethostbyname(servers[x])))
    except:
        pass

# start them at about the same time
for x in serverthreads:
    x.start()

# wait for 5 seconds and then kill the test
def quit():
    os._exit(0)

t = threading.Timer(5.0, quit)
t.start()

# wait for the threads to finish at the same time
for x in serverthreads:
    x.join()

os._exit(0)
