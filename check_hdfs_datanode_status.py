#!/usr/bin/env python
"""
Author: Sravan Kumar Tammineni
Nagios-hdfs-plugin for monitoring datanode status
Tested on Centos
Prerequisites: BeautifulSoup python package should be installed on host
"""
from BeautifulSoup import BeautifulSoup
import urllib2
import os
import sys

host = os.uname()[1]

try:
    response = urllib2.urlopen('http://{0}:50075/'.format(host), timeout = 10)
    res_code = response.getcode()
    data = response.read()
    parse_data = BeautifulSoup(data)
    status = parse_data.body.find('div', attrs={'class':'page-header'}).text
except:
    print "CRITICAL - Unable to get status from WebUI"
    sys.exit(2)


if status == "DataNode on":
    print "OK - DataNode is ON"
    sys.exit(0)
elif (status != "DataNode on" or res_code == 200):
    print "CRITICAL - Data node is OFF"
    sys.exit(2)
else:
    print "UNKNOWN - Node status is unknown"
    sys.exit(3)
