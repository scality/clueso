#!/bin/bash

# Alluxio entrypoint actions:
# Only set ALLUXIO_RAM_FOLDER if tiered storage isn't explicitly configured
  if [[ -z "${ALLUXIO_WORKER_TIEREDSTORE_LEVEL0_DIRS_PATH}" ]]; then
    # Docker will set this tmpfs up by default. Its size is configurable through the
    # --shm-size argument to docker run
    export ALLUXIO_RAM_FOLDER=${ALLUXIO_RAM_FOLDER:-/dev/shm}
  fi

  home=/opt/alluxio
  cd ${home}

  # List of environment variables which go in alluxio-env.sh instead of
  # alluxio-site.properties
  alluxio_env_vars=(
    ALLUXIO_CLASSPATH
    ALLUXIO_HOSTNAME
    ALLUXIO_JARS
    ALLUXIO_JAVA_OPTS
    ALLUXIO_MASTER_JAVA_OPTS
    ALLUXIO_PROXY_JAVA_OPTS
    ALLUXIO_RAM_FOLDER
    ALLUXIO_USER_JAVA_OPTS
    ALLUXIO_WORKER_JAVA_OPTS
  )

  for keyvaluepair in $(env | grep "ALLUXIO_"); do
    # split around the "="
    key=$(echo ${keyvaluepair} | cut -d= -f1)
    value=$(echo ${keyvaluepair} | cut -d= -f2)
    if [[ ! "${alluxio_env_vars[*]}" =~ "${key}" ]]; then
      confkey=$(echo ${key} | sed "s/_/./g" | tr '[:upper:]' '[:lower:]')
      echo "${confkey}=${value}" >> conf/alluxio-site.properties
    else
      echo "export ${key}=${value}" >> conf/alluxio-env.sh
    fi
  done

  bin/alluxio formatWorker
  bin/alluxio-worker.sh &

   # Spark worker actions:
  . "/spark/sbin/spark-config.sh"

  . "/spark/bin/load-spark-env.sh"

  mkdir -p $SPARK_WORKER_LOG

  /spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG/spark-worker.out

