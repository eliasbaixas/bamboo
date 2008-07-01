#!/usr/bin/env python
import sys; import sha; import socket; import struct
if (len(sys.argv) < 2):
    print "Usage:", sys.argv[0], "<hostname> <port>"
    sys.exit(1)
hostname = sys.argv[1]
port = sys.argv[2]
ip = socket.gethostbyname(hostname)
ipbytes = socket.inet_aton(ip)
format = "H%dp" % (len(ipbytes) + 1)
data = struct.pack(format, int(port), ipbytes)
result = sha.new(data).digest()
print "0x%08x %08x %08x %08x %08x" % struct.unpack('5I', result)
