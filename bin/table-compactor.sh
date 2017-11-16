#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

java -cp $DIR/../lib/clueso-tool.jar -Dspark.ui.port=4057 com.scality.clueso.tools.MetadataTableCompactorTool $* 2>&1 | grep -z "Compactor"
