#!/bin/bash
set -e

# Start Hadoop (necessary for Hive)
$HADOOP_HOME/sbin/start-dfs.sh

# Initialize the Hive Metastore schema if not already initialized
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
if schematool -dbType postgres -info | grep -q "Schema version"; then
    echo "Hive schema already initialized."
else
    echo "Initializing Hive schema..."
    schematool -dbType postgres -initSchema || echo "Schema already initialized or encountered an error"
fi
echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"

# Start Hive Metastore and HiveServer2 in background
/opt/hive/bin/hive --service metastore &
METASTORE_PID=$!

/opt/hive/bin/hive --service hiveserver2 &
HIVESERVER2_PID=$!

# Wait for both services to exit (keeps the container alive)
wait $METASTORE_PID $HIVESERVER2_PID
