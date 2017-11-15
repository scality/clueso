#!/bin/bash

GRAPHITE_HOST=${GRAPHITE_HOST:-graphite}
GRAPHITE_PORT=${GRAPHITE_PORT:-2003}
java -cp ../lib/clueso-tool.jar -DgraphiteHost=$GRAPHITE_HOST -DgraphitePort=$GRAPHITE_PORT -Dspark.ui.port=4057 com.scality.clueso.tools.MetadataStorageInfoTool $1 $2 $3