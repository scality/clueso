FROM java:openjdk-8-jdk
MAINTAINER Vianney Rancurel <vr@scality.com>

ENV HADOOP_VERSION 2.8.1
ENV SPARK_BRANCH 2.1.1
ENV SPARK_HOME /spark/
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_VERSION 2.1.1_2.11

# once spark is packaged with hadoop 2.8 we can stop compiling
# https://issues.apache.org/jira/browse/HADOOP-12963
RUN apt-get update \
    && apt-get install -y supervisor curl build-essential \
    python git maven --no-install-recommends \
    && mkdir -p /var/log/supervisor \
    && curl --retry 6 \
    -fSL https://www.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz -o /tmp/hadoop.tar.gz \
    && apt-get install -y supervisor curl --no-install-recommends \
    && mkdir -p /var/log/supervisor \
    && tar -xvf /tmp/hadoop.tar.gz -C /opt/ \
    && rm /tmp/hadoop.tar.gz \
    && mkdir -p /apps/build/ \
    && cd /apps/build/ \
    && git clone https://github.com/apache/spark.git \
    && mv spark spark-$SPARK_VERSION \
    && cd spark-$SPARK_VERSION \
    && git checkout tags/v$SPARK_BRANCH \
    && ./dev/change-scala-version.sh 2.11 \
    && cd /apps/build/spark-$SPARK_VERSION  \
    && /apps/build/spark-$SPARK_VERSION/dev/make-distribution.sh \
        --name spark-$SPARK_VERSION \
        --tgz -Phadoop-2.7 -Dhadoop.version=$HADOOP_VERSION \
        -DskipTests -DXms=2g \
    && tar -zxvf /apps/build/spark-$SPARK_VERSION/spark-$SPARK_BRANCH-bin-spark-$SPARK_VERSION.tgz -C /tmp/ \
    && mkdir -p $SPARK_HOME \
    && cp -rf /tmp/spark-$SPARK_BRANCH-bin-spark-$SPARK_VERSION/* $SPARK_HOME \
    && rm -rf -- /tmp/spark-$SPARK_BRANCH-bin-spark-$SPARK_VERSION/ \
    && mkdir -p /clueso

COPY clueso /clueso
RUN mkdir -p /clueso/heapdumps && mkdir -p /clueso/conf/
COPY application.conf /clueso/conf/
COPY conf /spark/conf/ 

CMD bash
