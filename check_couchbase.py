#!/usr/bin/python
"""
Nagios-couchbase-plugin for monitoring status of servers in cluster
Tested on Centos
"""
import json
import sys
from optparse import OptionParser
import requests

nagios_codes = {
    'OK': 0,
    'WARNING': 1,
    'CRITICAL': 2,
    'UNKNOWN': 3,
}

def active_servers(node, TR, user, password):
    url = ''.join(['http://', node,':8091','/pools/default/'])
    data = json.loads(requests.get(url, auth=(user, password)).content)
    counter = 0
    for key in data['nodes']:
        if key['clusterMembership'] == 'inactiveFailed':
            counter += 1
    count = counter

    if count > TR:
        print "CRITICAL - Total number of failed nodes in the cluster:", count
        sys.exit(nagios_codes['CRITICAL'])
    elif count == TR:
        print "WARNING - Total number of failed nodes in the cluster:", count
        sys.exit(nagios_codes['WARNING'])
    elif count < TR:
        print "OK - All nodes are normal"
        sys.exit(nagios_codes['OK'])
    else:
        print "UNKNOWN - Status unknown"
        sys.exit(nagios_codes['UNKNOWN'])

if __name__ == '__main__':
    try:
        parser = OptionParser(usage='usage: %prog -n <couchbase node> -t <threshold value> -u <username> -p <password>', version=1.0)
        parser.add_option('-n', dest='ip', default='localhost', help='couchbase node')
        parser.add_option('-t', type='int', dest='threshold', default=1, help='Threshold value')
        parser.add_option('-u', dest='username', help='username for couchbase server')
        parser.add_option('-p', dest='password', help='password for couchbase server')
        (options, args) = parser.parse_args()
        node = options.ip
        TR = options.threshold
        user = options.username
        password = options.password
        active_servers(node, TR, user, password)
    except KeyboardInterrupt:
        print 'Ctrl^C Caught! Exiting...'
        sys.exit(-1)
