FROM scality/clueso-spark-base:latest as base
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
    netcat less inetutils-ping \
    libsasl2-modules && \
    rm -rf /var/lib/apt/lists/* && \
    cd && wget https://bootstrap.pypa.io/ez_setup.py && python ez_setup.py && cd -

# Overall ENV vars
ENV LIVY_BRANCH tags/v0.4.0-incubating

# Set install path for Livy
ENV LIVY_APP_PATH /apps/livy

# Set build path for Livy
ENV LIVY_BUILD_PATH /apps/build/livy

# Set Hadoop config directory
ENV HADOOP_CONF_DIR /etc/hadoop/conf

# Clone Livy repository
RUN mkdir -p /apps/build && \
    cd /apps/build && \
    git clone https://github.com/apache/incubator-livy.git $LIVY_BUILD_PATH && \
    cd $LIVY_BUILD_PATH && \
    git checkout $LIVY_BRANCH && \
    mvn -DskipTests -Dspark.version=2.1.1  clean package && \
    unzip $LIVY_BUILD_PATH/assembly/target/*.zip -d /apps && \
    mv /apps/livy* $LIVY_APP_PATH && \
    mkdir -p $LIVY_APP_PATH/logs

COPY --from=base /clueso/ /clueso/
COPY --from=base /spark/ /spark/

COPY conf /apps/livy/conf
COPY conf/log4j.properties /spark/conf/log4j.properties
COPY conf/spark-defaults.conf /spark/conf/spark-defaults.conf

# Copy custom files, set permissions
COPY entrypoint.sh .

RUN chmod +x entrypoint.sh

# Expose port
EXPOSE 8998 38600-38632 4040-4056 5005

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
