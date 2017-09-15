#!/bin/bash

java -cp clueso-1.0-SNAPSHOT-tool.jar -DgraphiteHost=graphite -DgraphitePort=2003 -Dspark.ui.port=4057 com.scality.clueso.tools.MetadataStorageInfoTool $1 $2 $3