#!/bin/bash
#if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    #echo "Formatting NameNode..."
    #hdfs namenode -format
#fi
#!/bin/bash

# Format the namenode if it's the first run
if [ ! -f /opt/hadoop/data/nameNode/current/VERSION ]; then
  hdfs namenode -format
fi

# Start the namenode
hdfs namenode