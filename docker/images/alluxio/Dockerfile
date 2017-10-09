FROM openjdk:8

ENV ALLUXIO_VERSION 1.5.0

RUN mkdir -p /opt && cd /opt && wget http://downloads.alluxio.org/downloads/files/"$ALLUXIO_VERSION"/alluxio-"$ALLUXIO_VERSION"-bin.tar.gz && \
       tar -zxvf alluxio-"$ALLUXIO_VERSION"-bin.tar.gz && \
       mv alluxio-"$ALLUXIO_VERSION" alluxio && \
       rm alluxio-"$ALLUXIO_VERSION"-bin.tar.gz

COPY bin/* /opt/alluxio/bin/
COPY conf /opt/alluxio/conf/
COPY entrypoint.sh /

EXPOSE 19998 19999

ENTRYPOINT ["/entrypoint.sh"]
