#!/usr/bin/python

import socket
import sys
import time

HOST, PORT = '10.19.14.122', 53003
data = 'this is test.'

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))

while True:
    sock.send(data + "\n")
    print "send:%s" % data
    time.sleep(1)

sock.close()
