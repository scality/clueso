FROM bde2020/hadoop-base:1.1.0-hadoop2.8-java8

ENV ENABLE_INIT_DAEMON true
ENV INIT_DAEMON_BASE_URI http://identifier/init-daemon
ENV INIT_DAEMON_STEP spark_master_init

COPY wait-for-step.sh /
COPY execute-step.sh /
COPY finish-step.sh /

RUN apt-get update && apt-get install -y wget

# packages
RUN apt-get install -yq --no-install-recommends --force-yes \
    wget \
    python \
    git \
    maven curl  \
    iproute2 \
    libsvn1 \
    libcurl3 \
    zip unzip \
    netcat inetutils-ping \
    libsasl2-modules && \
    rm -rf /var/lib/apt/lists/* && \
    cd && wget https://bootstrap.pypa.io/ez_setup.py && python ez_setup.py && cd -

RUN mkdir -p /apps/build/ && \
		    cd /apps/build/ && \
		    git clone https://github.com/apache/spark.git && \
		    mv spark spark-2.1.1_2.11 && \
		    cd spark-2.1.1_2.11 && \
		    git checkout tags/v2.1.1 && \
		    ./dev/change-scala-version.sh 2.11


RUN cd /apps/build/spark-2.1.1_2.11 && /apps/build/spark-2.1.1_2.11/dev/make-distribution.sh --name spark-2.1.1_2.11 --tgz -Phadoop-2.7 -Dhadoop.version=2.8.1  -DskipTests -DXms=2g

ENV SPARK_HOME /spark/

RUN tar -zxvf /apps/build/spark-2.1.1_2.11/spark-2.1.1-bin-spark-2.1.1_2.11.tgz -C /tmp/ && mkdir -p $SPARK_HOME && cp -rf /tmp/spark-2.1.1-bin-spark-2.1.1_2.11/* $SPARK_HOME && \
		    rm -rf -- /tmp/spark-2.1.1-bin-spark-2.1.1_2.11/


#
#
#      && wget https://dl.dropboxusercontent.com/u/4882345/packages/spark-2.1.2-SNAPSHOT-bin-spark-2.1.0-hive.tgz \
#      && apt-get --purge remove -y wget \
#      && tar -xvzf spark-2.1.2-SNAPSHOT-bin-spark-2.1.0-hive.tgz \
#      && mv spark-2.1.2-SNAPSHOT-bin-spark-2.1.0-hive spark \
#      && rm spark-2.1.2-SNAPSHOT-bin-spark-2.1.0-hive.tgz \
#      && cd /
#


COPY hive-site.xml /spark/conf/
COPY conf/* /spark/conf/

COPY entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["entrypoint.sh"]

