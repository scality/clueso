FROM ubuntu

# packages
RUN apt-get update && apt-get install -yq --no-install-recommends --force-yes \
    wget \
    python \
    git \
    openjdk-8-jdk \
    maven curl  \
    iproute2 \
    libsvn1 \
    libcurl3 \
    zip unzip \
    netcat inetutils-ping \
    libsasl2-modules && \
    rm -rf /var/lib/apt/lists/* && \
    cd && wget https://bootstrap.pypa.io/ez_setup.py && python ez_setup.py && cd -

# Overall ENV vars
ENV SPARK_VERSION 2.1.1
ENV LIVY_BRANCH tags/v0.4.0-incubating

# Set install path for Livy
ENV LIVY_APP_PATH /apps/livy

# Set build path for Livy
ENV LIVY_BUILD_PATH /apps/build/livy

# Set Hadoop config directory
ENV HADOOP_CONF_DIR /etc/hadoop/conf

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Set Spark home directory
ENV SPARK_HOME /apps/spark

# Spark ENV vars
#ENV SPARK_VERSION_STRING spark-$SPARK_VERSION-bin-hadoop2.7
ENV SPARK_VERSION_STRING spark-$SPARK_VERSION-bin-without-hadoop
ENV SPARK_DOWNLOAD_URL http://d3kbcqa49mib13.cloudfront.net/$SPARK_VERSION_STRING.tgz
ENV SPARK_BUILD_NAME=spark-2.1.1_2.11
ENV SPARK_BUILD_DIR=/apps/build/$SPARK_BUILD_NAME

# try to build spark...
RUN mkdir -p /apps/build/ && \
    cd /apps/build/ && \
    git clone https://github.com/apache/spark.git $SPARK_BUILD_NAME && \
    cd $SPARK_BUILD_DIR && \
    git checkout tags/v2.1.1 && \
    ./dev/change-scala-version.sh 2.11

RUN cd $SPARK_BUILD_DIR && ./dev/make-distribution.sh --name $SPARK_BUILD_NAME --tgz -Phadoop-2.7 -Phive -Phive-thriftserver -Pmesos -Dhadoop.version=2.8.0 -DzincPort=3036 -DskipTests -Pyarn -e

RUN tar -zxvf /apps/build/$SPARK_BUILD_NAME/spark-2.1.1-bin-$SPARK_BUILD_NAME.tgz -C /tmp/ && \
    mkdir -p $SPARK_HOME && \
    cp -rf /tmp/spark-2.1.1-bin-$SPARK_BUILD_NAME/* $SPARK_HOME && \
    rm -rf -- /tmp/spark-2.1.1-bin-spark-2.1.1_2.11/

# Clone Livy repository
RUN mkdir -p /apps/build && \
    cd /apps/build && \
	git clone https://github.com/apache/incubator-livy.git $LIVY_BUILD_PATH

RUN cd $LIVY_BUILD_PATH && git checkout $LIVY_BRANCH && mvn -DskipTests -Dspark.version=2.1.1 clean package && \
    unzip $LIVY_BUILD_PATH/assembly/target/*.zip -d /apps && \
    mv /apps/livy* $LIVY_APP_PATH && \
	mkdir -p $LIVY_APP_PATH/logs && \
	mkdir -p /apps/spark-modules/

# # Alluxio # #
RUN mkdir -p /opt && cd /opt && wget http://downloads.alluxio.org/downloads/files/1.5.0/alluxio-1.5.0-bin.tar.gz && \
       tar -zxvf alluxio-1.5.0-bin.tar.gz && \
       mv alluxio-1.5.0 alluxio && \
       rm alluxio-1.5.0-bin.tar.gz

# Add custom files, set permissions
ADD entrypoint.sh .

RUN chmod +x entrypoint.sh

# Expose port
EXPOSE 8998 38600-38632 4040-4056 5005

ENTRYPOINT ["/entrypoint.sh"]