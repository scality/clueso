FROM ubuntu

RUN apt-get update

RUN apt-get install -y software-properties-common

RUN add-apt-repository -y ppa:webupd8team/java

RUN apt-get update

RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections

RUN apt-get install -y oracle-java8-installer

# packages
RUN apt-get update && apt-get install -yq --no-install-recommends --force-yes \
    wget \
    python \
    git \
    maven \
    libsvn1 \
    libcurl3 \
    zip unzip \
    netcat inetutils-ping \
    less lsof vim \
    libsasl2-modules && \
    rm -rf /var/lib/apt/lists/* && \
    cd && wget https://bootstrap.pypa.io/ez_setup.py && python ez_setup.py && cd -

#export SPARK_VERSION=2.1.1
#export LIVY_BRANCH=tags/v0.3.0
#export LIVY_APP_PATH=/apps/livy
#export LIVY_BUILD_PATH=/apps/build/incubator-livy
#export HADOOP_CONF_DIR=/etc/hadoop/conf
#export SPARK_HOME=/apps/spark
#export SPARK_VERSION_STRING=spark-$SPARK_VERSION-bin-hadoop2.7
#export SPARK_DOWNLOAD_URL=http://d3kbcqa49mib13.cloudfront.net/$SPARK_VERSION_STRING.tgz


# Overall ENV vars
ENV SPARK_VERSION 2.1.1
#ENV LIVY_BRANCH = tags/v0.4.0-incubating-rc1
ENV LIVY_BRANCH = tags/v0.3.0

# Set install path for Livy
ENV LIVY_APP_PATH /apps/livy

# Set build path for Livy
ENV LIVY_BUILD_PATH /apps/build/livy

# Set Hadoop config directory
ENV HADOOP_CONF_DIR /etc/hadoop/conf

# Set Spark home directory
ENV SPARK_HOME /apps/spark


# Spark ENV vars
ENV SPARK_VERSION_STRING spark-$SPARK_VERSION-bin-hadoop2.7
ENV SPARK_DOWNLOAD_URL http://d3kbcqa49mib13.cloudfront.net/$SPARK_VERSION_STRING.tgz


# try to build spark...
#RUN mkdir -p /apps/build/ && \
#    cd /apps/build/ && \
#    git clone https://github.com/apache/spark.git && \
#    mv spark spark-2.1.1_2.10 && \
#    cd spark-2.1.1_2.10 && \
#    git checkout tags/v2.1.1 && \
#    ./dev/change-scala-version.sh 2.10 && \
#    ./dev/make-distribution.sh --name spark-2.1.1_2.10 --tgz -Phadoop-2.7 -Phive -Phive-thriftserver -Pmesos -Pyarn -Dscala-2.10 -DzincPort=3036 -DskipTests
#
#RUN tar -zxvf /apps/build/spark-2.1.1_2.10/spark-2.1.1-bin-spark-2.1.1_2.10.tgz -C 


# ./build/mvn -Phadoop-2.7 -Psparkr -Phive -Phive-thriftserver -Pyarn -Pmesos -Dscala-2.10  -DzincPort=3036 -DskipTests clean package


# Download and unzip Spark
RUN wget $SPARK_DOWNLOAD_URL && \
    mkdir -p $SPARK_HOME && \
    tar xvf $SPARK_VERSION_STRING.tgz -C /tmp && \
    cp -rf /tmp/$SPARK_VERSION_STRING/* $SPARK_HOME && \
    rm -rf -- /tmp/$SPARK_VERSION_STRING && \
    rm spark-$SPARK_VERSION-bin-hadoop2.7.tgz


# mvn -DskipTests -Dscala.version=2.11.8 -Dscala.binary.version=2.11 -Dspark.version=$SPARK_VERSION clean package && \

# Clone Livy repository
RUN mkdir -p /apps/build && \
    cd /apps/build && \
	git clone https://github.com/apache/incubator-livy.git $LIVY_BUILD_PATH && \
	cd $LIVY_BUILD_PATH && \
	git checkout -b $LIVY_BRANCH && \
    mvn -DskipTests -Dspark.version=2.1.1 clean package && \
    unzip $LIVY_BUILD_PATH/assembly/target/*.zip -d /apps && \
    mv /apps/livy* $LIVY_APP_PATH && \
	mkdir -p $LIVY_APP_PATH/logs && \
	mkdir -p /apps/spark-modules/

#    rm -rf $LIVY_BUILD_PATH && \

	
# Add custom files, set permissions
ADD entrypoint.sh .

RUN chmod +x entrypoint.sh

# Expose port
EXPOSE 8998

ENTRYPOINT ["/entrypoint.sh"]