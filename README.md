# NRPE-plugins
Customized scripts for monitoring

# Flink - Monitor jobs

./check_flink_jobs -u < username > -p < password > -P < port > -j < job_name > 

## Help
./check_flink_jobs --help

# Couchbase -  Monitor node status in the cluster

./check_couchbase.py -n < couchbase_node > -t < threshold_level > -u < username > -p < password >

## Help
./check_couchbase.py --help

# Cassandra - Monitor RW, connectivity status

./check_cassandra_connectivity.py -n < cassandra_node1, cassandra_node2, cassandra_node3,... >

# HDFS - Monitor namenode, journalnode and datanode status

./check_hdfs_namenode_safemode.py
./check_hdfs_journalnode_status.py
./check_hdfs_datanode_status.py
