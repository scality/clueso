FROM ubuntu

ARG ALLUXIO_TARBALL=http://downloads.alluxio.org/downloads/files/1.5.0/alluxio-1.5.0-bin.tar.gz

ADD ${ALLUXIO_TARBALL} /opt/

RUN apt-get update && apt-get install -yq --no-install-recommends --force-yes \
    openjdk-8-jdk && \
    rm -rf /var/cache/apt/* && \
    # if the tarball was remote, it needs to be untarred
    cd /opt && tar -zxvf alluxio-*gz && \
        ls && rm -rf alluxio-*gz && mv alluxio-* alluxio && \
        ln -s /opt/alluxio-1.5.0 /opt/alluxio

COPY bin/* /opt/alluxio/bin/
COPY conf /opt/alluxio/conf/
COPY entrypoint.sh /

EXPOSE 19998 19999

ENTRYPOINT ["/entrypoint.sh"]