#!/bin/bash

java -cp clueso-1.0-SNAPSHOT-tool.jar -Dspark.ui.port=4057 com.scality.clueso.tools.MetadataTableCompactorTool $* 2>&1 | grep -z "Compactor"
