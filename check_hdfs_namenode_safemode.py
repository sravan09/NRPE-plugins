#!/usr/bin/env python
"""
Author: Sravan Kumar Tammineni
Nagios-hdfs-plugin for monitoring namenode and safemode status
Tested on Centos
Prerequisites: BeautifulSoup python package should be installed on host
"""
import os
import sys
import urllib2
import json

host = os.uname()[1]

try:
    response = urllib2.urlopen("http://{0}:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus".format(host), timeout = 10)
    response1 = urllib2.urlopen("http://{0}:50070/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo".format(host), timeout = 10)
    res_code = response.getcode()
    res_code1 = response1.getcode()
    data = json.loads(response.read())
    data1 = json.loads(response1.read())
    status = data['beans'][0]['State']
    safemode_status = data1['beans'][0]['Safemode']
except:
    print "CRITICAL - Unable to access WebUI"
    sys.exit(2)

if (status == 'active' or status == 'standby'):
    if (safemode_status == ""):
        print "OK - Node is healthy / Mode:", status
        sys.exit(0)
    else:
        print "WARNING - Node is healthy but", safemode_status
        sys.exit(1)
elif ((status != 'active' or status != 'standby') and res_code == 200):
    print "CRITICAL - Node is not active or standby"
    sys.exit(2)
else:
    print "Unknown Status"
    sys.exit(3)
