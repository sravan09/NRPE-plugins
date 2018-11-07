#!/usr/bin/env pythoin
"""
Author: Sravan Kumar Tammineni
Nagios-cassandra-plugin for monitoring Read/Write status, connectivity between host and cassandra cluster
Tested on Centos
Prerequisites: This script requires "mon" named keyspace and table available in cassandra cluster
"""
import os
import sys
import time
import argparse
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.policies import DCAwareRoundRobinPolicy

KEYSPACE = "mon"
consistency_level = "one"
host_name = os.uname()[1]
epoch_ms = int(round(time.time() * 1000))

def write(session,epoch_ms,host_name):
    query = SimpleStatement("insert into mon.mon (hostname,time) values ('{0}', {1});".format(host_name, epoch_ms), consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    try:
    session.execute(query)
    except:
    print "CRITICAL - Unable to write to Cassandra"
        sys.exit(2)

def read(session,epoch_ms,host_name):
    query = SimpleStatement("select time from mon.mon where hostname = '{}';".format(host_name), consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    try:
    row1 = session.execute(query)
    except:
    print "CRITICAL - Unable to read from Cassandra"
        sys.exit(2)
    for user_row in row1:
        output = user_row.time
        if epoch_ms == output:
            print "OK - Cassandra health check is OK"
            sys.exit(0)
        elif epoch_ms != output:
            print "CRITICAL - Cassandra health check [READ Failure]"
            sys.exit(2)
        else:
            print "UNKNOWN - Unknown Status"
            sys.exit(3)

def connect2cluster(cas_hosts):
    cluster = Cluster(cas_hosts.split(","))
    session = cluster.connect()
    write(session,epoch_ms,host_name)
    read(session,epoch_ms,host_name)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='check cassandra connectivity')
        required = parser.add_argument_group('required arguments')
        optional = parser.add_argument_group('optional arguments')
        required.add_argument('-n','--nodes', help="cassandra nodes", required=True)
        args = parser.parse_args()
    #cas_hosts = args.nodes.split(",")
    cas_hosts = args.nodes
    connect2cluster(cas_hosts)
    except KeyboardInterrupt:
        print 'Ctrl^C Caught! Exiting...'
        sys.exit(-1)
